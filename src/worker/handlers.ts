export type JobHandler = (payload: unknown, ctx: { jobId: string; attempt: number }) => Promise<unknown>;

/**
 * Handler registry. Handlers MUST be safe to run more than once —
 * delivery is at-least-once (see README).
 */
export const handlers: Record<string, JobHandler> = {
  // echoes payload back as the result
  'echo': async (payload) => payload,

  // sleeps `ms` then returns payload; useful for crash/long-job tests
  'sleep': async (payload) => {
    const ms = typeof (payload as { ms?: number })?.ms === 'number' ? (payload as { ms: number }).ms : 100;
    await new Promise((r) => setTimeout(r, ms));
    return { slept: ms };
  },

  // always throws; exercises retry/backoff/dead-letter paths
  'fail': async () => {
    throw new Error('handler intentionally failed');
  },

  // succeeds only on or after `succeedOnAttempt` (from payload)
  'flaky': async (payload, { attempt }) => {
    const succeedOn = (payload as { succeedOnAttempt?: number })?.succeedOnAttempt ?? 2;
    if (attempt < succeedOn) throw new Error(`flaky failure on attempt ${attempt}`);
    return { succeededOn: attempt };
  },
};
