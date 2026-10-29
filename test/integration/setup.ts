import pg from 'pg';
import { Redis } from 'ioredis';
import { runMigrations } from '../../src/db/migrate.js';
import { QueueCoordinator } from '../../src/queue/redis.js';
import { createApp } from '../../src/api/app.js';
import { Worker } from '../../src/worker/worker.js';
import { createLogger } from '../../src/log.js';
import { loadConfig, type Config } from '../../src/config.js';

export const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://queueforge:queueforge@localhost:5432/queueforge';
export const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';

const log = createLogger({ component: 'test' });

export interface TestEnv {
  pool: pg.Pool;
  redis: Redis;
  queue: QueueCoordinator;
  app: ReturnType<typeof createApp>;
}

export function testConfig(overrides: Partial<Config> = {}): Config {
  return {
    ...loadConfig({}),
    databaseUrl: DATABASE_URL,
    redisUrl: REDIS_URL,
    workerId: `test-worker-${Math.random().toString(36).slice(2, 8)}`,
    workerConcurrency: 4,
    workerLeaseMs: 10_000,
    workerPollIntervalMs: 25,
    workerReaperIntervalMs: 200,
    workerSweepIntervalMs: 200,
    retryBaseDelayMs: 100,
    shutdownGraceMs: 3_000,
    ...overrides,
  };
}

export async function makeEnv(): Promise<TestEnv> {
  const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 10 });
  const redis = new Redis(REDIS_URL, { maxRetriesPerRequest: null });
  const queue = new QueueCoordinator(redis, log);
  await runMigrations(pool);
  const app = createApp({ pool, queue, redis, log });
  return { pool, redis, queue, app };
}

export async function resetState(env: TestEnv): Promise<void> {
  await env.pool.query('TRUNCATE job_attempts, jobs');
  await env.redis.flushdb();
}

export async function closeEnv(env: TestEnv): Promise<void> {
  await env.redis.quit();
  await env.pool.end();
}

export function startWorker(
  env: TestEnv,
  configOverrides: Partial<Config> = {},
  handlerMap?: ConstructorParameters<typeof Worker>[4],
): Worker {
  const w = new Worker(env.pool, env.queue, testConfig(configOverrides), log, handlerMap);
  void w.run();
  return w;
}

export async function stopWorkers(workers: Worker[]): Promise<void> {
  for (const w of workers) w.stop();
  // run() drains; give it a moment
  await new Promise((r) => setTimeout(r, 200));
}

export async function waitFor(
  predicate: () => Promise<boolean> | boolean,
  timeoutMs = 10_000,
  intervalMs = 50,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error('waitFor timed out');
}

export async function jobState(env: TestEnv, id: string) {
  const { rows } = await env.pool.query('SELECT * FROM jobs WHERE id = $1', [id]);
  return rows[0] ?? null;
}
