import pg from 'pg';
import type { Config } from '../config.js';
import type { Logger } from '../log.js';
import { JobState } from '../core/states.js';
import {
  completeJob,
  failJob,
  findNotEnqueued,
  getJob,
  leaseJob,
  markEnqueued,
  reapExpiredLeases,
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
    // lease reaper + un-enqueued sweeper run alongside the claim loop
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
      log.warn({ err }, 'lease failed (postgres); job stays claimable');
      await this.queue.releaseInflight(jobId).catch(() => {});
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
      await this.queue.releaseInflight(jobId).catch(() => {});
    }
  }

  /** Recover jobs whose lease expired (crashed or hung workers). */
  private async reap(): Promise<void> {
    try {
      const reaped = await reapExpiredLeases(this.pool);
      for (const job of reaped) {
        try {
          await this.queue.enqueue(job.id, job.run_at);
          await markEnqueued(this.pool, job.id);
          this.log.warn({ jobId: job.id, attempt: job.attempt }, 'reaped expired lease');
        } catch (err) {
          this.log.warn({ err, jobId: job.id }, 'requeue after reap failed; sweeper will retry');
        }
      }
    } catch (err) {
      this.log.warn({ err }, 'reaper tick failed');
    }
  }

  /**
   * Re-push jobs that were persisted but never reached Redis, and reconcile
   * orphaned inflight entries (worker died between RPOPLPUSH and the
   * Postgres lease — the row is still queued but nobody will run it).
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
      if (inflightIds.length > 0) {
        const { rows } = await this.pool.query<{ id: string; state: JobState; run_at: Date }>(
          'SELECT id, state, run_at FROM jobs WHERE id = ANY($1)',
          [inflightIds],
        );
        const byId = new Map(rows.map((r) => [r.id, r]));
        for (const id of inflightIds) {
          const row = byId.get(id);
          if (!row || row.state === JobState.Queued || row.state === JobState.Delayed) {
            // orphaned: never leased (or recovered) — return to a real queue
            await this.queue.releaseInflight(id);
            if (row) await this.queue.enqueue(id, row.run_at);
            this.log.warn({ jobId: id }, 'repaired orphaned inflight entry');
          }
          // leased jobs genuinely in-flight are left alone
        }
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
