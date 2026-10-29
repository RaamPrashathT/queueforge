import { Redis } from 'ioredis';
import type { Logger } from '../log.js';

export const KEYS = {
  ready: 'qf:ready',
  delayed: 'qf:delayed',
  inflight: 'qf:inflight',
  claims: 'qf:claims', // hash: jobId -> epoch ms when claimed (for orphan grace)
} as const;

// Atomically promote due delayed jobs into the ready list and pop the next
// ready id onto inflight, stamping the claim time.
// FIFO: ids are LPUSH'd onto ready (head); RPOPLPUSH pops the tail — oldest first.
const CLAIM_LUA = `
local now = tonumber(ARGV[1])
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 100)
for _, id in ipairs(due) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('LPUSH', KEYS[2], id)
end
local jobId = redis.call('RPOPLPUSH', KEYS[2], KEYS[3])
if jobId then
  redis.call('HSET', KEYS[4], jobId, now)
end
return jobId
`;

// Remove a specific id from inflight and forget its claim stamp.
const RELEASE_LUA = `
redis.call('HDEL', KEYS[2], ARGV[1])
return redis.call('LREM', KEYS[1], 1, ARGV[1])
`;

// Remove from inflight AND put it back on the ready list, atomically —
// used when the Postgres lease step fails so the job is never stranded.
const REQUEUE_LUA = `
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('LREM', KEYS[1], 1, ARGV[1])
redis.call('LPUSH', KEYS[2], ARGV[1])
return 1
`;

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
      await this.redis.lpush(KEYS.ready, jobId);
    }
  }

  /** Pop the next claimable job id, promoting due delayed jobs atomically. */
  async claimNext(): Promise<string | null> {
    const id = (await this.redis.eval(
      CLAIM_LUA,
      4,
      KEYS.delayed,
      KEYS.ready,
      KEYS.inflight,
      KEYS.claims,
      Date.now().toString(),
    )) as string | null;
    return id;
  }

  /** Forget an inflight id (finalize path). */
  async releaseInflight(jobId: string): Promise<void> {
    await this.redis.eval(RELEASE_LUA, 2, KEYS.inflight, KEYS.claims, jobId);
  }

  /** Atomically move an inflight id back to the ready list. */
  async requeueToReady(jobId: string): Promise<void> {
    await this.redis.eval(REQUEUE_LUA, 3, KEYS.inflight, KEYS.ready, KEYS.claims, jobId);
  }

  async inflightDepth(): Promise<number> {
    return this.redis.llen(KEYS.inflight);
  }

  /** All ids currently on the inflight list. */
  async inflightIds(): Promise<string[]> {
    return this.redis.lrange(KEYS.inflight, 0, -1);
  }

  /** Epoch ms at which each id was claimed; missing entry = unknown/legacy. */
  async claimTimes(ids: string[]): Promise<Map<string, number>> {
    const map = new Map<string, number>();
    if (ids.length === 0) return map;
    const vals = await this.redis.hmget(KEYS.claims, ...ids);
    ids.forEach((id, i) => {
      const v = vals[i];
      if (v != null) map.set(id, Number(v));
    });
    return map;
  }

  /** Forget stale claim stamps (cleanup for terminal/missing rows). */
  async clearClaims(ids: string[]): Promise<void> {
    if (ids.length === 0) return;
    await this.redis.hdel(KEYS.claims, ...ids);
  }
}

export function createRedisClient(url: string, log: Logger): InstanceType<typeof Redis> {
  const redis = new Redis(url, {
    maxRetriesPerRequest: null, // let callers decide; avoids ioredis throwing mid-loop
    enableReadyCheck: true,
    lazyConnect: false,
  });
  redis.on('error', (err: Error) => log.warn({ err }, 'redis connection error'));
  redis.on('reconnecting', () => log.warn('redis reconnecting'));
  return redis;
}

export async function waitForRedis(
  redis: InstanceType<typeof Redis>,
  timeoutMs = 30_000,
): Promise<void> {
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
