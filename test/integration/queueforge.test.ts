import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  closeEnv,
  jobState,
  makeEnv,
  resetState,
  startWorker,
  stopWorkers,
  testConfig,
  waitFor,
  type TestEnv,
} from './setup.js';
import { leaseJob } from '../../src/core/jobs.js';
import type { JobHandler } from '../../src/worker/handlers.js';

let env: TestEnv;

beforeAll(async () => {
  env = await makeEnv();
});

beforeEach(async () => {
  await resetState(env);
});

afterAll(async () => {
  await closeEnv(env);
});

const postJob = (body: unknown, headers: Record<string, string> = {}) =>
  env.app.request('/jobs', {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  });

describe('job submission', () => {
  it('accepts a job and exposes it via GET', async () => {
    const res = await postJob({ type: 'echo', payload: { a: 1 } });
    expect(res.status).toBe(201);
    const { job } = await res.json();
    const get = await env.app.request(`/jobs/${job.id}`);
    expect(get.status).toBe(200);
    const { job: fetched } = await get.json();
    expect(fetched.state).toBe('queued');
  });

  it('is idempotent on idempotency key', async () => {
    const r1 = await postJob({ type: 'echo', payload: { a: 1 }, idempotencyKey: 'k-1' });
    const r2 = await postJob({ type: 'echo', payload: { a: 1 }, idempotencyKey: 'k-1' });
    const r3 = await postJob({ type: 'echo', payload: { b: 2 } }, { 'idempotency-key': 'k-1' });
    expect(r1.status).toBe(201);
    expect(r2.status).toBe(200);
    expect(r3.status).toBe(200);
    const j1 = (await r1.json()).job;
    const j2 = (await r2.json()).job;
    const j3 = (await r3.json()).job;
    expect(j2.id).toBe(j1.id);
    expect(j3.id).toBe(j1.id);
    const { rows } = await env.pool.query('SELECT count(*)::int AS c FROM jobs');
    expect(rows[0].c).toBe(1);
  });

  it('honours delayed execution', async () => {
    const res = await postJob({ type: 'echo', payload: {}, delayMs: 600 });
    const { job } = await res.json();
    const worker = startWorker(env);
    try {
      await waitFor(async () => (await jobState(env, job.id))?.state === 'succeeded');
      const done = await jobState(env, job.id);
      expect(new Date(done.run_at).getTime() - new Date(done.created_at).getTime()).toBeGreaterThanOrEqual(500);
    } finally {
      await stopWorkers([worker]);
    }
  });
});

describe('workers', () => {
  it('executes each job exactly once across concurrent workers', async () => {
    const N = 20;
    const executions: string[] = [];
    const spy: JobHandler = async (payload, { jobId }) => {
      executions.push(jobId);
      await new Promise((r) => setTimeout(r, 20));
      return payload;
    };
    const workers = [
      startWorker(env, { workerId: 'w1', workerConcurrency: 3 }, { track: spy }),
      startWorker(env, { workerId: 'w2', workerConcurrency: 3 }, { track: spy }),
      startWorker(env, { workerId: 'w3', workerConcurrency: 3 }, { track: spy }),
    ];
    try {
      const ids: string[] = [];
      for (let i = 0; i < N; i++) {
        const res = await postJob({ type: 'track', payload: { i } });
        ids.push((await res.json()).job.id);
      }
      await waitFor(async () => {
        const { rows } = await env.pool.query(
          "SELECT count(*)::int AS c FROM jobs WHERE state = 'succeeded'",
        );
        return rows[0].c === N;
      });
      expect(executions.sort()).toEqual([...ids].sort());
    } finally {
      await stopWorkers(workers);
    }
  });

  it('retries a flaky job with backoff until success', async () => {
    const res = await postJob({
      type: 'flaky',
      payload: { succeedOnAttempt: 2 },
      maxAttempts: 3,
    });
    const { job } = await res.json();
    const worker = startWorker(env);
    try {
      await waitFor(async () => (await jobState(env, job.id))?.state === 'succeeded');
      const done = await jobState(env, job.id);
      expect(done.attempt).toBe(2);
    } finally {
      await stopWorkers([worker]);
    }
  });

  it('moves a permanently failing job to dead after max_attempts', async () => {
    const res = await postJob({ type: 'fail', payload: {}, maxAttempts: 2 });
    const { job } = await res.json();
    const worker = startWorker(env);
    try {
      await waitFor(async () => (await jobState(env, job.id))?.state === 'dead', 15_000);
      const done = await jobState(env, job.id);
      expect(done.attempt).toBe(2);
      const { rows } = await env.pool.query(
        "SELECT count(*)::int AS c FROM job_attempts WHERE job_id = $1 AND outcome = 'failed'",
        [job.id],
      );
      expect(rows[0].c).toBe(2);
    } finally {
      await stopWorkers([worker]);
    }
  });

  it('recovers a job when the leasing worker dies mid-execution', async () => {
    // Simulate a crash: lease the job directly with a short lease and never finish it.
    const res = await postJob({ type: 'echo', payload: { x: 1 } });
    const { job } = await res.json();
    // pull it out of redis like a worker would, then "die" after leasing
    const claimed = await env.queue.claimNext();
    expect(claimed).toBe(job.id);
    const leased = await leaseJob(env.pool, job.id, 'dead-worker', 400);
    expect(leased).toBe(true);
    // dead-worker never completes it; lease expires

    const recovery = startWorker(env, { workerId: 'recovery-worker' });
    try {
      await waitFor(
        async () => (await jobState(env, job.id))?.state === 'succeeded',
        15_000,
      );
      const { rows } = await env.pool.query(
        'SELECT outcome FROM job_attempts WHERE job_id = $1 ORDER BY id',
        [job.id],
      );
      expect(rows[0].outcome).toBe('lease_expired');
      expect(rows.at(-1)!.outcome).toBe('completed');
    } finally {
      await stopWorkers([recovery]);
    }
  });

  it('dead-letters a lease-expired job already at max_attempts', async () => {
    const res = await postJob({ type: 'sleep', payload: { ms: 5000 }, maxAttempts: 1 });
    const { job } = await res.json();
    const claimed = await env.queue.claimNext();
    expect(claimed).toBe(job.id);
    await leaseJob(env.pool, job.id, 'crashed-worker', 300);
    const worker = startWorker(env, { workerId: 'reaper-worker' });
    try {
      // attempt was already 1 = max_attempts when the lease expired, so the
      // reaper must dead-letter it, not requeue it (poison-pill fix).
      await waitFor(async () => (await jobState(env, job.id))?.state === 'dead');
      const { rows } = await env.pool.query(
        "SELECT outcome FROM job_attempts WHERE job_id = $1 AND outcome = 'lease_expired'",
        [job.id],
      );
      expect(rows).toHaveLength(1);
    } finally {
      await stopWorkers([worker]);
    }
  });
});

describe('resilience', () => {
  it('does not lose a job when redis enqueue fails at submit time', async () => {
    // Simulate Redis being down at submit: write the row with enqueued=false
    // by calling submitJob directly, skipping the redis push.
    const { submitJob } = await import('../../src/core/jobs.js');
    const { job } = await submitJob(env.pool, { type: 'echo', payload: { late: true } });
    expect(job.enqueued).toBe(false);

    const worker = startWorker(env);
    try {
      // sweeper should pick it up and process it
      await waitFor(async () => (await jobState(env, job.id))?.state === 'succeeded', 15_000);
    } finally {
      await stopWorkers([worker]);
    }
  });

  it('health endpoints respond', async () => {
    const h = await env.app.request('/healthz');
    expect(h.status).toBe(200);
    const r = await env.app.request('/readyz');
    expect(r.status).toBe(200);
    expect((await r.json()).postgres).toBe(true);
  });
});

describe('adversarial regressions', () => {
  it('processes ready jobs FIFO, not LIFO', async () => {
    for (let i = 0; i < 3; i++) {
      await env.queue.enqueue(`job-fifo-${i}`, new Date());
    }
    const order = [
      await env.queue.claimNext(),
      await env.queue.claimNext(),
      await env.queue.claimNext(),
    ];
    expect(order).toEqual(['job-fifo-0', 'job-fifo-1', 'job-fifo-2']);
  });

  it('returns a job to ready when leaseJob fails (no silent loss)', async () => {
    const res = await postJob({ type: 'echo', payload: {} });
    const { job } = await res.json();
    const claimed = await env.queue.claimNext();
    expect(claimed).toBe(job.id);

    // exercise the worker's failure path: requeue to ready, atomically
    await env.queue.requeueToReady(claimed!);
    const next = await env.queue.claimNext();
    expect(next).toBe(job.id);
  });

  it('cleans terminal jobs out of inflight (no leak)', async () => {
    const res = await postJob({ type: 'echo', payload: {} });
    const { job } = await res.json();
    const worker = startWorker(env);
    try {
      await waitFor(async () => (await jobState(env, job.id))?.state === 'succeeded');
      // simulate the leak window: id back on inflight after completion
      await env.redis.rpush('qf:inflight', job.id);
      await waitFor(async () => (await env.queue.inflightIds()).length === 0);
      expect(await env.queue.inflightIds()).toEqual([]);
    } finally {
      await stopWorkers([worker]);
    }
  });

  it('rejects malformed job ids with 400, not 500', async () => {
    const res = await env.app.request('/jobs/not-a-uuid');
    expect(res.status).toBe(400);
    const res2 = await env.app.request('/jobs/not-a-uuid/attempts');
    expect(res2.status).toBe(400);
  });

  it('fences stale workers: an old attempt cannot finalize a newer one', async () => {
    const res = await postJob({ type: 'echo', payload: {} });
    const { job } = await res.json();
    const cfg = testConfig();
    // worker-1 leases attempt 1
    await env.queue.claimNext();
    expect(await leaseJob(env.pool, job.id, cfg.workerId, 300)).toBe(true);
    const staleJob = (await jobState(env, job.id))!;
    expect(staleJob.attempt).toBe(1);
    // lease expires; recovery worker leases attempt 2 (slow handler so we
    // can observe the leased state before it completes)
    const slowEcho: JobHandler = async () => {
      await new Promise((r) => setTimeout(r, 2000));
      return { ok: true };
    };
    const recovery = startWorker(env, { workerId: 'recovery' }, { echo: slowEcho });
    try {
      await waitFor(async () => {
        const s = await jobState(env, job.id);
        return s?.state === 'leased' && s.attempt === 2;
      });
      // stale worker now "finishes" attempt 1 — must NOT finalize attempt 2
      const { completeJob } = await import('../../src/core/jobs.js');
      await completeJob(env.pool, staleJob, cfg.workerId, { stale: true }, new Date());
      const after = await jobState(env, job.id);
      expect(after.state).toBe('leased');
      expect(after.attempt).toBe(2);
    } finally {
      await stopWorkers([recovery]);
    }
  });

  it('does not repair inflight entries inside the claim grace window', async () => {
    const res = await postJob({ type: 'echo', payload: {} });
    const { job } = await res.json();
    const claimed = await env.queue.claimNext();
    expect(claimed).toBe(job.id);
    // sweeper runs every 200ms; claim grace is 500ms — inflight id must
    // survive sweeps until the grace period passes or it is leased.
    const worker = startWorker(env, { workerId: 'grace-worker' });
    try {
      await waitFor(async () => (await jobState(env, job.id))?.state === 'succeeded');
    } finally {
      await stopWorkers([worker]);
    }
    // the job ran exactly once (no sweeper-induced duplicate)
    const { rows } = await env.pool.query(
      "SELECT count(*)::int AS c FROM job_attempts WHERE job_id = $1 AND outcome = 'completed'",
      [job.id],
    );
    expect(rows[0].c).toBe(1);
  });
});
