function int(env: string | undefined, fallback: number): number {
  const n = env === undefined ? NaN : Number.parseInt(env, 10);
  return Number.isFinite(n) ? n : fallback;
}

export interface Config {
  port: number;
  databaseUrl: string;
  redisUrl: string;
  workerId: string;
  workerConcurrency: number;
  workerLeaseMs: number;
  workerReaperIntervalMs: number;
  workerSweepIntervalMs: number;
  workerPollIntervalMs: number;
  retryBaseDelayMs: number;
  shutdownGraceMs: number;
  logLevel: string;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  return {
    port: int(env.PORT, 3000),
    databaseUrl: env.DATABASE_URL ?? 'postgres://queueforge:queueforge@localhost:5432/queueforge',
    redisUrl: env.REDIS_URL ?? 'redis://localhost:6379',
    workerId: env.WORKER_ID ?? `worker-${process.pid}`,
    workerConcurrency: int(env.WORKER_CONCURRENCY, 4),
    workerLeaseMs: int(env.WORKER_LEASE_MS, 30_000),
    workerReaperIntervalMs: int(env.WORKER_REAPER_INTERVAL_MS, 5_000),
    workerSweepIntervalMs: int(env.WORKER_SWEEP_INTERVAL_MS, 5_000),
    workerPollIntervalMs: int(env.WORKER_POLL_INTERVAL_MS, 500),
    retryBaseDelayMs: int(env.RETRY_BASE_DELAY_MS, 1_000),
    shutdownGraceMs: int(env.SHUTDOWN_GRACE_MS, 10_000),
    logLevel: env.LOG_LEVEL ?? 'info',
  };
}
