import { Redis } from 'ioredis';
import type { Logger } from '../log.js';

export const KEYS = {
  ready: 'qf:ready',
  delayed: 'qf:delayed',
  inflight: 'qf:inflight',
} as const;

// Atomically promote due delayed jobs into the ready list and pop the next
// ready id onto the inflight list. Single Lua call = atomic; no check-then-set.
const CLAIM_LUA = `
local now = tonumber(ARGV[1])
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 100)
for _, id in ipairs(due) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('RPUSH', KEYS[2], id)
end
local jobId = redis.call('RPOPLPUSH', KEYS[2], KEYS[3])
return jobId
`;

// Remove a specific id from inflight (finalize path).
const RELEASE_LUA = `return redis.call('LREM', KEYS[1], 1, ARGV[1])`;

export class QueueCoordinator {
  constructor(
    private readonly redis: InstanceType<typeof Redis>,
    private readonly log: Logger,
  ) {}

  /** Push a job id to the right coordination structure. Atomic single op. */
  async enqueue(jobId: string, runAt: Date): Promise<void> {
    const delay = runAt.getTime() - Date.now();
    if (delay > 0) {
      await this.redis.zadd(KEYS.delayed, runAt.getTime().toString(), jobId);
    } else {
      await this.redis.rpush(KEYS.ready, jobId);
    }
  }

  /** Pop the next claimable job id, promoting due delayed jobs atomically. */
  async claimNext(): Promise<string | null> {
    const id = (await this.redis.eval(
      CLAIM_LUA,
      3,
      KEYS.delayed,
      KEYS.ready,
      KEYS.inflight,
      Date.now().toString(),
    )) as string | null;
    return id;
  }

  /** Return an id we pulled to inflight but could not lease in Postgres. */
  async releaseInflight(jobId: string): Promise<void> {
    await this.redis.eval(RELEASE_LUA, 1, KEYS.inflight, jobId);
  }


  async inflightDepth(): Promise<number> {
    return this.redis.llen(KEYS.inflight);
  }

  /** All ids currently on the inflight list. */
  async inflightIds(): Promise<string[]> {
    return this.redis.lrange(KEYS.inflight, 0, -1);
  }
}

export function createRedisClient(url: string, log: Logger): Redis {
  const redis = new Redis(url, {
    maxRetriesPerRequest: null, // let callers decide; avoids ioredis throwing mid-loop
    enableReadyCheck: true,
    lazyConnect: false,
  });
  redis.on('error', (err: Error) => log.warn({ err }, 'redis connection error'));
  redis.on('reconnecting', () => log.warn('redis reconnecting'));
  return redis;
}

export async function waitForRedis(redis: InstanceType<typeof Redis>, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastErr: unknown;
  while (Date.now() < deadline) {
    try {
      await redis.ping();
      return;
    } catch (err) {
      lastErr = err;
      await new Promise((r) => setTimeout(r, 500));
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error('redis not reachable');
}
