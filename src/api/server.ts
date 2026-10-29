import { serve } from '@hono/node-server';
import { loadConfig } from '../config.js';
import { createLogger } from '../log.js';
import { createPool, waitForPostgres } from '../db/client.js';
import { runMigrations } from '../db/migrate.js';
import { createRedisClient, QueueCoordinator, waitForRedis } from '../queue/redis.js';
import { createApp } from './app.js';

const config = loadConfig();
const log = createLogger({ component: 'api' });

const pool = createPool(config.databaseUrl);
const redis = createRedisClient(config.redisUrl, log);
const queue = new QueueCoordinator(redis, log);

await waitForPostgres(pool);
await waitForRedis(redis);
await runMigrations(pool);

const app = createApp({ pool, queue, redis, log });
const server = serve({ fetch: app.fetch, port: config.port });
log.info({ port: config.port }, 'api listening');

async function shutdown(signal: string) {
  log.info({ signal }, 'shutting down');
  server.close();
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
