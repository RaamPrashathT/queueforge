export const MIGRATIONS: { id: string; sql: string }[] = [
  {
    id: '0001_init',
    sql: `
      CREATE TABLE IF NOT EXISTS jobs (
        id                UUID PRIMARY KEY,
        type              TEXT NOT NULL,
        payload           JSONB NOT NULL DEFAULT '{}',
        state             TEXT NOT NULL,
        attempt           INTEGER NOT NULL DEFAULT 0,
        max_attempts      INTEGER NOT NULL DEFAULT 3,
        run_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
        lease_expires_at  TIMESTAMPTZ,
        leased_by         TEXT,
        idempotency_key   TEXT,
        enqueued          BOOLEAN NOT NULL DEFAULT false,
        result            JSONB,
        error             TEXT,
        created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
      );
      CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency_key_uniq
        ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;
      CREATE INDEX IF NOT EXISTS jobs_state_idx ON jobs (state);
      CREATE INDEX IF NOT EXISTS jobs_lease_idx
        ON jobs (lease_expires_at) WHERE state = 'leased';
      CREATE INDEX IF NOT EXISTS jobs_not_enqueued_idx
        ON jobs (state) WHERE enqueued = false;

      CREATE TABLE IF NOT EXISTS job_attempts (
        id          BIGSERIAL PRIMARY KEY,
        job_id      UUID NOT NULL REFERENCES jobs (id),
        attempt     INTEGER NOT NULL,
        worker_id   TEXT NOT NULL,
        started_at  TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        outcome     TEXT NOT NULL,
        error       TEXT
      );
      CREATE INDEX IF NOT EXISTS job_attempts_job_idx ON job_attempts (job_id);
    `,
  },
];

export const SCHEMA_MIGRATIONS_DDL = `
  CREATE TABLE IF NOT EXISTS schema_migrations (
    id TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );
`;
