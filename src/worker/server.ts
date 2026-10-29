import { serve } from '@hono/node-server';
import { Hono } from 'hono';
import { loadConfig } from '../config.js';
import { createLogger } from '../log.js';
import { createPool, waitForPostgres } from '../db/client.js';
import { runMigrations } from '../db/migrate.js';
import { createRedisClient, QueueCoordinator, waitForRedis } from '../queue/redis.js';
import { Worker } from './worker.js';

const config = loadConfig();
const log = createLogger({ component: 'worker', workerId: config.workerId });

const pool = createPool(config.databaseUrl);
const redis = createRedisClient(config.redisUrl, log);
const queue = new QueueCoordinator(redis, log);

await waitForPostgres(pool);
await waitForRedis(redis);
await runMigrations(pool);

// minimal health endpoint so orchestrators can probe workers too
const health = new Hono();
health.get('/healthz', (c) => c.json({ ok: true, workerId: config.workerId }));
const healthPort = Number(process.env.WORKER_HEALTH_PORT ?? 3001);
serve({ fetch: health.fetch, port: healthPort });

const worker = new Worker(pool, queue, config, log);
const runPromise = worker.run();
log.info(
  { concurrency: config.workerConcurrency, leaseMs: config.workerLeaseMs },
  'worker started',
);

let shuttingDown = false;
async function shutdown(signal: string) {
  if (shuttingDown) return;
  shuttingDown = true;
  log.info({ signal }, 'shutdown requested; draining');
  worker.stop();
  const deadline = Date.now() + config.shutdownGraceMs;
  while (Date.now() < deadline) {
    if (await Promise.race([runPromise.then(() => true), new Promise((r) => setTimeout(r, 100, false))])) break;
  }
  try {
    await redis.quit();
  } catch {
    /* already gone */
  }
  await pool.end();
  process.exit(0);
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

await runPromise;
