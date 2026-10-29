import pg from 'pg';
import { JobState } from './states.js';

export interface Job {
  id: string;
  type: string;
  payload: unknown;
  state: JobState;
  attempt: number;
  max_attempts: number;
  run_at: Date;
  lease_expires_at: Date | null;
  leased_by: string | null;
  idempotency_key: string | null;
  enqueued: boolean;
  result: unknown;
  error: string | null;
  created_at: Date;
  updated_at: Date;
}

export interface SubmitJobInput {
  type: string;
  payload?: unknown;
  maxAttempts?: number;
  delayMs?: number;
  idempotencyKey?: string;
}

/**
 * Insert a job durably. When an idempotency key collides, returns the
 * existing job instead — the caller gets a 200 + the original job.
 * The job is inserted with enqueued=false; the caller pushes to Redis
 * and then calls markEnqueued (or the sweeper does it later).
 */
export async function submitJob(
  pool: pg.Pool,
  input: SubmitJobInput,
): Promise<{ job: Job; deduplicated: boolean }> {
  const state = input.delayMs && input.delayMs > 0 ? JobState.Delayed : JobState.Queued;
  const { rows } = await pool.query<Job>(
    `INSERT INTO jobs (id, type, payload, state, max_attempts, run_at, idempotency_key)
     VALUES (gen_random_uuid(), $1, $2, $3, $4, now() + ($5 || ' milliseconds')::interval, $6)
     ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
     DO NOTHING
     RETURNING *`,
    [
      input.type,
      JSON.stringify(input.payload ?? {}),
      state,
      input.maxAttempts ?? 3,
      input.delayMs ?? 0,
      input.idempotencyKey ?? null,
    ],
  );
  if (rows[0]) return { job: rows[0], deduplicated: false };

  const { rows: existing } = await pool.query<Job>(
    'SELECT * FROM jobs WHERE idempotency_key = $1',
    [input.idempotencyKey!],
  );
  return { job: existing[0]!, deduplicated: true };
}

export async function markEnqueued(pool: pg.Pool, jobId: string): Promise<void> {
  await pool.query('UPDATE jobs SET enqueued = true, updated_at = now() WHERE id = $1', [jobId]);
}

export async function getJob(pool: pg.Pool, id: string): Promise<Job | null> {
  const { rows } = await pool.query<Job>('SELECT * FROM jobs WHERE id = $1', [id]);
  return rows[0] ?? null;
}

/**
 * Atomic claim: only succeeds if the job is still claimable.
 * Returns true iff THIS worker moved the job to 'leased'.
 */
export async function leaseJob(
  pool: pg.Pool,
  jobId: string,
  workerId: string,
  leaseMs: number,
): Promise<boolean> {
  const { rowCount } = await pool.query(
    `UPDATE jobs
     SET state = $1, leased_by = $2,
         lease_expires_at = now() + ($3 || ' milliseconds')::interval,
         attempt = attempt + 1,
         updated_at = now()
     WHERE id = $4 AND state IN ($5, $6) AND run_at <= now()`,
    [JobState.Leased, workerId, leaseMs, jobId, JobState.Queued, JobState.Delayed],
  );
  return (rowCount ?? 0) === 1;
}

/** leased -> succeeded (guarded on this worker's lease) */
export async function completeJob(
  pool: pg.Pool,
  job: Job,
  workerId: string,
  result: unknown,
  startedAt: Date,
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rowCount } = await client.query(
      `UPDATE jobs SET state = $1, result = $2, error = NULL,
              lease_expires_at = NULL, leased_by = NULL, updated_at = now()
       WHERE id = $3 AND state = $4 AND leased_by = $5`,
      [JobState.Succeeded, JSON.stringify(result ?? null), job.id, JobState.Leased, workerId],
    );
    if ((rowCount ?? 0) !== 1) {
      await client.query('ROLLBACK');
      return; // lease was lost/reaped; do not record a bogus completion
    }
    await client.query(
      `INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, outcome)
       VALUES ($1, $2, $3, $4, 'completed')`,
      [job.id, job.attempt, workerId, startedAt],
    );
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

/**
 * leased -> dead (attempts exhausted) or leased -> delayed (retry).
 * Returns the terminal/action state chosen, or null if the lease was lost.
 */
export async function failJob(
  pool: pg.Pool,
  job: Job,
  workerId: string,
  error: string,
  retryBaseDelayMs: number,
  startedAt: Date,
): Promise<{ next: JobState; runAt?: Date } | null> {
  const exhausted = job.attempt >= job.max_attempts;
  const backoffMs = retryBaseDelayMs * 2 ** (job.attempt - 1);
  const jitterMs = Math.floor(Math.random() * retryBaseDelayMs);
  const runAt = new Date(Date.now() + backoffMs + jitterMs);
  const next = exhausted ? JobState.Dead : JobState.Delayed;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rowCount } = await client.query(
      `UPDATE jobs SET state = $1, error = $2, run_at = $3,
              lease_expires_at = NULL, leased_by = NULL,
              enqueued = false, updated_at = now()
       WHERE id = $4 AND state = $5 AND leased_by = $6`,
      [next, error, exhausted ? job.run_at : runAt, job.id, JobState.Leased, workerId],
    );
    if ((rowCount ?? 0) !== 1) {
      await client.query('ROLLBACK');
      return null;
    }
    await client.query(
      `INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, outcome, error)
       VALUES ($1, $2, $3, $4, 'failed', $5)`,
      [job.id, job.attempt, workerId, startedAt, error],
    );
    await client.query('COMMIT');
    return exhausted ? { next } : { next, runAt };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Reaper: atomically recover jobs whose lease expired. Each row is reset
 * to queued/enqueued=false and a crashed attempt is recorded, all in one
 * transaction per row. Guarded on state='leased' so a finishing worker and
 * the reaper can't both win.
 */
export async function reapExpiredLeases(pool: pg.Pool, limit = 100): Promise<Job[]> {
  const { rows: expired } = await pool.query<Job>(
    `SELECT * FROM jobs
     WHERE state = $1 AND lease_expires_at < now()
     ORDER BY lease_expires_at ASC
     LIMIT $2
     FOR UPDATE SKIP LOCKED`,
    [JobState.Leased, limit],
  );
  const reaped: Job[] = [];
  for (const job of expired) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const { rowCount } = await client.query(
        `UPDATE jobs SET state = $1, lease_expires_at = NULL, leased_by = NULL,
                enqueued = false, updated_at = now()
         WHERE id = $2 AND state = $3`,
        [JobState.Queued, job.id, JobState.Leased],
      );
      if ((rowCount ?? 0) === 1) {
        await client.query(
          `INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, outcome, error)
           VALUES ($1, $2, $3, $4, 'lease_expired', 'lease expired before job finished')`,
          [job.id, job.attempt, job.leased_by ?? 'unknown', job.lease_expires_at ?? new Date()],
        );
        await client.query('COMMIT');
        reaped.push({ ...job, state: JobState.Queued });
      } else {
        await client.query('ROLLBACK');
      }
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }
  return reaped;
}

/**
 * Sweeper: jobs written to Postgres but never pushed to Redis
 * (API died between write and push, or Redis was down at submit).
 */
export async function findNotEnqueued(pool: pg.Pool, limit = 200): Promise<Job[]> {
  const { rows } = await pool.query<Job>(
    `SELECT * FROM jobs
     WHERE enqueued = false AND state IN ($1, $2)
     ORDER BY created_at ASC
     LIMIT $3`,
    [JobState.Queued, JobState.Delayed, limit],
  );
  return rows;
}
