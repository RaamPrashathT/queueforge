# queueforge

Distributed background job processing in Node.js + TypeScript.

- **Hono** REST API for job submission and inspection
- **PostgreSQL** is the source of truth for job state and attempt history
- **Redis** is used only for queue coordination (job IDs, no payloads)
- Independent worker processes with configurable concurrency

## Quick start

```bash
docker compose up --build        # api on :3000, 2 workers, postgres, redis
docker compose up --scale worker=5  # more workers
```

```bash
# submit a job
curl -X POST localhost:3000/jobs \
  -H 'content-type: application/json' \
  -H 'idempotency-key: order-42-email' \
  -d '{"type":"echo","payload":{"hello":"world"},"maxAttempts":3,"delayMs":0}'

curl localhost:3000/jobs/<id>
curl localhost:3000/jobs/<id>/attempts
curl localhost:3000/healthz   # liveness
curl localhost:3000/readyz    # checks postgres + redis
```

Built-in job types: `echo`, `sleep {ms}`, `fail`, `flaky {succeedOnAttempt}`.
Register real handlers in `src/worker/handlers.ts`.

## Architecture

```
POST /jobs ──► INSERT jobs (queued|delayed, enqueued=false)
             ──► Redis push (ready list | delayed zset)
             ──► UPDATE enqueued=true

worker: Lua script ──► promote due delayed → ready, RPOPLPUSH ready → inflight
                       (FIFO: ids LPUSH'd at head, popped from tail;
                        claim stamped in qf:claims)
       guarded UPDATE ──► queued → leased (attempt+1, lease_expires_at)
       heartbeat: renew lease while the handler runs
       handler() ──► leased → succeeded   (fenced on attempt+leased_by)
                 └─► leased → delayed (retry, backoff + jitter, 1h cap) | dead
reaper:  ONE tx (SELECT FOR UPDATE SKIP LOCKED) — expired leases:
         attempt < max → queued + 'lease_expired' attempt
         attempt = max → dead (no poison-pill loop)
sweeper: enqueued=false → push to Redis
         inflight ids: terminal/missing → released (no leak);
         queued beyond CLAIM_GRACE_MS → orphan repaired;
         inside grace window → untouched (no steal-race)
```

### Lifecycle states

`delayed → queued → leased → succeeded` · `leased → failed → delayed` (retry) ·
`failed → dead` (attempts exhausted) · `leased → queued` (lease expired, reaped).

Every transition is a single guarded `UPDATE ... WHERE state = <expected> AND
leased_by = <worker> AND attempt = <n>` — the atomicity of the claim prevents
two workers running the same job, and the attempt fence prevents a stale
(reaped) handler from finalizing a newer attempt. Leases are renewed by a
heartbeat while a handler runs, so long jobs aren't reaped mid-flight.

## Delivery guarantees

**At-least-once.** A worker can crash after a handler's side effect but before
recording completion; the job is then recovered and run again. Design handlers
to make their effects idempotent (e.g. UPSERT, natural keys). There is **no**
exactly-once claim.

Honest failure windows:

| Window | Mitigation |
|---|---|
| API dies between Postgres write and Redis push | `enqueued` flag + sweeper re-push |
| Worker dies after RPOPLPUSH, before lease | sweeper reconciles orphaned `inflight` ids against Postgres state |
| Worker dies mid-handler | lease expiry → reaper → requeue (duplicate execution possible) |
| Redis data lost entirely | Postgres rows remain; sweeper rebuilds coordination state |
| Postgres unavailable | submissions fail with 503 — no job is "accepted" that doesn't exist |

## Configuration

| Env | Default | Purpose |
|---|---|---|
| `PORT` | 3000 | API port |
| `DATABASE_URL` | — | Postgres DSN |
| `REDIS_URL` | — | Redis URL |
| `WORKER_CONCURRENCY` | 4 | jobs processed in parallel per worker |
| `WORKER_LEASE_MS` | 30000 | claim lease duration |
| `WORKER_REAPER_INTERVAL_MS` | 5000 | expired-lease scan cadence |
| `WORKER_SWEEP_INTERVAL_MS` | 5000 | recovery sweep cadence |
| `RETRY_BASE_DELAY_MS` | 1000 | backoff base (`base·2^attempt` + jitter, capped at 1h) |
| `CLAIM_GRACE_MS` | 10000 | inflight claims younger than this are never treated as orphans |
| `SHUTDOWN_GRACE_MS` | 10000 | drain deadline on SIGTERM |
| `LOG_LEVEL` | info | pino level |

## Development

```bash
npm install
docker compose up -d postgres redis
npm run migrate && npm run dev:api    # or dev:worker
npm run test                          # integration tests (need postgres+redis up)
npm run typecheck
```

Graceful shutdown: workers stop claiming on SIGTERM, finish in-flight jobs up to
`SHUTDOWN_GRACE_MS`, then exit; remaining leases expire and are recovered by peers.
