import pg from 'pg';
import { JobState } from './states.js';

const MAX_BACKOFF_MS = 60 * 60 * 1000; // 1 hour cap on exponential backoff

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

/** Mark a job not-enqueued so the sweeper re-pushes it (e.g. lease attempt failed). */
export async function markNotEnqueued(pool: pg.Pool, jobId: string): Promise<void> {
  await pool.query(
    `UPDATE jobs SET enqueued = false, updated_at = now()
     WHERE id = $1 AND state IN ($2, $3)`,
    [jobId, JobState.Queued, JobState.Delayed],
  );
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

/**
 * Extend an active lease — heartbeat for long-running handlers.
 * Fenced on worker + attempt so only the current leaseholder renews.
 */
export async function renewLease(
  pool: pg.Pool,
  jobId: string,
  workerId: string,
  attempt: number,
  leaseMs: number,
): Promise<boolean> {
  const { rowCount } = await pool.query(
    `UPDATE jobs
     SET lease_expires_at = now() + ($1 || ' milliseconds')::interval,
         updated_at = now()
     WHERE id = $2 AND state = $3 AND leased_by = $4 AND attempt = $5`,
    [leaseMs, jobId, JobState.Leased, workerId, attempt],
  );
  return (rowCount ?? 0) === 1;
}

/**
 * leased -> succeeded. Fenced on attempt so a stale handler that was
 * reaped can never finalize a newer attempt.
 */
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
       WHERE id = $3 AND state = $4 AND leased_by = $5 AND attempt = $6`,
      [JobState.Succeeded, JSON.stringify(result ?? null), job.id, JobState.Leased, workerId, job.attempt],
    );
    if ((rowCount ?? 0) !== 1) {
      await client.query('ROLLBACK');
      return; // lease lost/reaped or a newer attempt took over
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
 * leased -> dead (attempts exhausted) or leased -> delayed (retry,
 * exponential backoff clamped to MAX_BACKOFF_MS).
 * Fenced on attempt like completeJob.
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
  const backoffMs = Math.min(
    retryBaseDelayMs * 2 ** (job.attempt - 1),
    MAX_BACKOFF_MS,
  );
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
       WHERE id = $4 AND state = $5 AND leased_by = $6 AND attempt = $7`,
      [next, error, exhausted ? job.run_at : runAt, job.id, JobState.Leased, workerId, job.attempt],
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

export interface ReapedJob {
  id: string;
  run_at: Date;
  outcome: 'requeued' | 'dead';
}

/**
 * Reaper: atomically recover jobs whose lease expired. SELECT + transitions
 * happen in ONE transaction so FOR UPDATE SKIP LOCKED actually holds across
 * the batch — no two reapers touch the same rows.
 * Jobs at max_attempts are dead-lettered instead of requeued (stops the
 * poison-pill crash loop).
 */
export async function reapExpiredLeases(pool: pg.Pool, limit = 100): Promise<ReapedJob[]> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows: expired } = await client.query<Job>(
      `SELECT * FROM jobs
       WHERE state = $1 AND lease_expires_at < now()
       ORDER BY lease_expires_at ASC
       LIMIT $2
       FOR UPDATE SKIP LOCKED`,
      [JobState.Leased, limit],
    );
    const results: ReapedJob[] = [];
    for (const job of expired) {
      const exhausted = job.attempt >= job.max_attempts;
      const next = exhausted ? JobState.Dead : JobState.Queued;
      const { rowCount } = await client.query(
        `UPDATE jobs SET state = $1, lease_expires_at = NULL, leased_by = NULL,
                enqueued = false,
                error = CASE WHEN $2 THEN 'lease expired at max_attempts' ELSE error END,
                updated_at = now()
         WHERE id = $3 AND state = $4`,
        [next, exhausted, job.id, JobState.Leased],
      );
      if ((rowCount ?? 0) !== 1) continue;
      await client.query(
        `INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, outcome, error)
         VALUES ($1, $2, $3, $4, 'lease_expired', 'lease expired before job finished')`,
        [job.id, job.attempt, job.leased_by ?? 'unknown', job.lease_expires_at ?? new Date()],
      );
      results.push({ id: job.id, run_at: job.run_at, outcome: exhausted ? 'dead' : 'requeued' });
    }
    await client.query('COMMIT');
    return results;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Sweeper: jobs written to Postgres but never pushed to Redis
 * (API died between write and push, or Redis was down at submit,
 * or a lease attempt failed and marked it not-enqueued).
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

export interface InflightRow {
  id: string;
  state: JobState;
  run_at: Date;
}

/** Fetch the states of a set of job ids (inflight reconciliation). */
export async function getJobStates(
  pool: pg.Pool,
  ids: string[],
): Promise<Map<string, InflightRow>> {
  if (ids.length === 0) return new Map();
  const { rows } = await pool.query<InflightRow>(
    'SELECT id, state, run_at FROM jobs WHERE id = ANY($1)',
    [ids],
  );
  return new Map(rows.map((r) => [r.id, r]));
}
