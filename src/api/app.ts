import { Hono } from 'hono';
import { randomUUID } from 'node:crypto';
import type pg from 'pg';
import type { QueueCoordinator } from '../queue/redis.js';
import { getJob, markEnqueued, submitJob } from '../core/jobs.js';
import type { Logger } from '../log.js';
import { Redis } from 'ioredis';

export interface AppDeps {
  pool: pg.Pool;
  queue: QueueCoordinator;
  redis: InstanceType<typeof Redis>;
  log: Logger;
}

export function createApp({ pool, queue, redis, log }: AppDeps) {
  const app = new Hono();

  // request id + structured access log
  app.use('*', async (c, next) => {
    const requestId = c.req.header('x-request-id') ?? randomUUID();
    c.set('requestId' as never, requestId as never);
    const start = Date.now();
    await next();
    c.header('x-request-id', requestId);
    log.info(
      {
        requestId,
        method: c.req.method,
        path: c.req.path,
        status: c.res.status,
        ms: Date.now() - start,
      },
      'request',
    );
  });

  app.get('/healthz', (c) => c.json({ ok: true }));

  app.get('/readyz', async (c) => {
    let pgOk = false;
    let redisOk = false;
    try {
      await pool.query('SELECT 1');
      pgOk = true;
    } catch {
      /* reported below */
    }
    try {
      await redis.ping();
      redisOk = true;
    } catch {
      /* reported below */
    }
    const ok = pgOk && redisOk;
    return c.json({ ok, postgres: pgOk, redis: redisOk }, ok ? 200 : 503);
  });

  app.post('/jobs', async (c) => {
    let body: unknown;
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: 'invalid JSON body' }, 400);
    }
    const b = body as Record<string, unknown>;
    if (typeof b?.type !== 'string' || b.type.length === 0) {
      return c.json({ error: '`type` (non-empty string) is required' }, 400);
    }
    const maxAttempts = b.maxAttempts === undefined ? 3 : Number(b.maxAttempts);
    const delayMs = b.delayMs === undefined ? 0 : Number(b.delayMs);
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 25) {
      return c.json({ error: '`maxAttempts` must be an integer 1..25' }, 400);
    }
    if (!Number.isFinite(delayMs) || delayMs < 0) {
      return c.json({ error: '`delayMs` must be a non-negative number' }, 400);
    }
    const idempotencyKey =
      typeof b.idempotencyKey === 'string'
        ? b.idempotencyKey
        : (c.req.header('idempotency-key') ?? undefined);

    // 1. Durable write first — Postgres is the source of truth.
    const { job, deduplicated } = await submitJob(pool, {
      type: b.type,
      payload: b.payload,
      maxAttempts,
      delayMs,
      idempotencyKey,
    });

    if (!deduplicated) {
      // 2. Best-effort enqueue to Redis; if it fails the job row stays
      //    enqueued=false and the worker-side sweeper re-pushes it.
      try {
        await queue.enqueue(job.id, job.run_at);
        await markEnqueued(pool, job.id);
      } catch (err) {
        log.warn({ err, jobId: job.id }, 'redis enqueue failed; sweeper will recover');
      }
    }

    return c.json({ job, deduplicated }, deduplicated ? 200 : 201);
  });

  app.get('/jobs/:id', async (c) => {
    const job = await getJob(pool, c.req.param('id'));
    if (!job) return c.json({ error: 'not found' }, 404);
    return c.json({ job });
  });

  app.get('/jobs/:id/attempts', async (c) => {
    const { rows } = await pool.query(
      'SELECT * FROM job_attempts WHERE job_id = $1 ORDER BY id',
      [c.req.param('id')],
    );
    return c.json({ attempts: rows });
  });

  return app;
}
