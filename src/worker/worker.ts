import pg from 'pg';
import type { Config } from '../config.js';
import type { Logger } from '../log.js';
import { JobState, TERMINAL_STATES } from '../core/states.js';
import {
  completeJob,
  failJob,
  findNotEnqueued,
  getJob,
  getJobStates,
  leaseJob,
  markEnqueued,
  markNotEnqueued,
  reapExpiredLeases,
  renewLease,
  type Job,
} from '../core/jobs.js';
import type { QueueCoordinator } from '../queue/redis.js';
import { handlers, type JobHandler } from './handlers.js';

export class Worker {
  private stopping = false;
  private inFlight = new Set<Promise<void>>();

  constructor(
    private readonly pool: pg.Pool,
    private readonly queue: QueueCoordinator,
    private readonly config: Config,
    private readonly log: Logger,
    private readonly handlerMap: Record<string, JobHandler> = handlers,
  ) {}

  async run(): Promise<void> {
    const reaper = this.every(this.config.workerReaperIntervalMs, () => this.reap());
    const sweeper = this.every(this.config.workerSweepIntervalMs, () => this.sweep());

    while (!this.stopping) {
      if (this.inFlight.size >= this.config.workerConcurrency) {
        await this.sleep(this.config.workerPollIntervalMs);
        continue;
      }
      let jobId: string | null;
      try {
        jobId = await this.queue.claimNext();
      } catch (err) {
        this.log.warn({ err }, 'redis claim failed; backing off');
        await this.sleep(this.config.workerPollIntervalMs * 4);
        continue;
      }
      if (!jobId) {
        await this.sleep(this.config.workerPollIntervalMs);
        continue;
      }
      const p = this.processOne(jobId).finally(() => this.inFlight.delete(p));
      this.inFlight.add(p);
    }

    clearInterval(reaper);
    clearInterval(sweeper);

    // graceful drain
    const deadline = Date.now() + this.config.shutdownGraceMs;
    while (this.inFlight.size > 0 && Date.now() < deadline) {
      await this.sleep(50);
    }
    this.log.info({ remaining: this.inFlight.size }, 'worker drained');
  }

  stop(): void {
    this.stopping = true;
  }

  private async processOne(jobId: string): Promise<void> {
    const log = this.log.child({ jobId, workerId: this.config.workerId });
    // Atomic lease: the single guarded UPDATE is what prevents two workers
    // from executing the same job concurrently.
    let leased: boolean;
    try {
      leased = await leaseJob(this.pool, jobId, this.config.workerId, this.config.workerLeaseMs);
    } catch (err) {
      log.warn({ err }, 'lease failed (postgres); returning job to ready');
      try {
        await this.queue.requeueToReady(jobId);
      } catch (rqErr) {
        log.warn({ err: rqErr }, 'redis requeue failed; marking not-enqueued for sweeper');
        await markNotEnqueued(this.pool, jobId).catch(() => {});
      }
      return;
    }
    if (!leased) {
      // someone else got it, or it was cancelled mid-flight
      await this.queue.releaseInflight(jobId).catch(() => {});
      return;
    }

    const job = await getJob(this.pool, jobId);
    if (!job) {
      await this.queue.releaseInflight(jobId).catch(() => {});
      return;
    }

    const handler = this.handlerMap[job.type];
    const startedAt = new Date();

    // Lease heartbeat: renew at ~1/3 of the lease so long handlers aren't reaped.
    const heartbeat = setInterval(() => {
      renewLease(this.pool, job.id, this.config.workerId, job.attempt, this.config.workerLeaseMs)
        .then((ok) => {
          if (!ok) log.warn('heartbeat lost lease');
        })
        .catch((err) => log.warn({ err }, 'heartbeat renew failed'));
    }, Math.max(1000, Math.floor(this.config.workerLeaseMs / 3)));

    try {
      if (!handler) throw new Error(`no handler registered for type '${job.type}'`);
      const result = await handler(job.payload, { jobId: job.id, attempt: job.attempt });
      await completeJob(this.pool, job, this.config.workerId, result, startedAt);
      log.info({ attempt: job.attempt }, 'job succeeded');
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      const outcome = await failJob(
        this.pool,
        job,
        this.config.workerId,
        message,
        this.config.retryBaseDelayMs,
        startedAt,
      );
      if (outcome?.next === JobState.Delayed && outcome.runAt) {
        try {
          await this.queue.enqueue(job.id, outcome.runAt);
          await markEnqueued(this.pool, job.id);
        } catch (enqueueErr) {
          log.warn({ err: enqueueErr }, 're-enqueue for retry failed; sweeper will recover');
        }
      }
      log.warn({ attempt: job.attempt, next: outcome?.next ?? 'lost-lease', err: message }, 'job failed');
    } finally {
      clearInterval(heartbeat);
      await this.queue.releaseInflight(jobId).catch(() => {});
    }
  }

  /** Recover jobs whose lease expired (crashed or hung workers). */
  private async reap(): Promise<void> {
    try {
      const reaped = await reapExpiredLeases(this.pool);
      for (const job of reaped) {
        if (job.outcome === 'dead') {
          this.log.warn({ jobId: job.id }, 'job dead-lettered at max_attempts after lease expiry');
          continue;
        }
        try {
          await this.queue.enqueue(job.id, job.run_at);
          await markEnqueued(this.pool, job.id);
          this.log.warn({ jobId: job.id }, 'reaped expired lease');
        } catch (err) {
          this.log.warn({ err, jobId: job.id }, 'requeue after reap failed; sweeper will retry');
        }
      }
    } catch (err) {
      this.log.warn({ err }, 'reaper tick failed');
    }
  }

  /**
   * Re-push jobs persisted but never pushed to Redis, and reconcile
   * orphaned inflight entries. An inflight id is only treated as orphaned
   * once its claim is older than `claimGraceMs` — this closes the race where
   * the sweeper steals a job a worker just claimed but hasn't leased yet.
   * Terminal (succeeded/dead) or missing rows are always released so the
   * inflight list can't leak.
   */
  private async sweep(): Promise<void> {
    try {
      const missing = await findNotEnqueued(this.pool);
      for (const job of missing) {
        try {
          await this.queue.enqueue(job.id, job.run_at);
          await markEnqueued(this.pool, job.id);
        } catch (err) {
          this.log.warn({ err, jobId: job.id }, 'sweep enqueue failed; will retry');
        }
      }

      const inflightIds = await this.queue.inflightIds();
      if (inflightIds.length === 0) return;

      const [byId, claimTimes] = await Promise.all([
        getJobStates(this.pool, inflightIds),
        this.queue.claimTimes(inflightIds),
      ]);
      const now = Date.now();
      for (const id of inflightIds) {
        const row = byId.get(id);
        if (!row || TERMINAL_STATES.has(row.state)) {
          // missing or finished — clean up inflight + claim stamp
          await this.queue.releaseInflight(id);
          this.log.warn({ jobId: id }, 'released stale inflight entry');
          continue;
        }
        if (row.state === JobState.Queued || row.state === JobState.Delayed) {
          const claimedAt = claimTimes.get(id);
          if (claimedAt !== undefined && now - claimedAt < this.config.claimGraceMs) {
            continue; // actively being leased — leave it alone
          }
          await this.queue.releaseInflight(id);
          await this.queue.enqueue(id, row.run_at);
          this.log.warn({ jobId: id }, 'repaired orphaned inflight entry');
        }
        // leased jobs genuinely in-flight are left alone
      }
    } catch (err) {
      this.log.warn({ err }, 'sweeper tick failed');
    }
  }

  private every(ms: number, fn: () => Promise<void>): NodeJS.Timeout {
    void fn();
    return setInterval(() => {
      if (!this.stopping) void fn();
    }, ms);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }
}

export type { Job };
