import pg from 'pg';
import { createLogger } from '../log.js';
import { loadConfig } from '../config.js';
import { MIGRATIONS, SCHEMA_MIGRATIONS_DDL } from './migrations.js';

export async function runMigrations(pool: pg.Pool): Promise<string[]> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Serialize concurrent migration runs across API/worker replicas.
    await client.query('SELECT pg_advisory_xact_lock(72747365)');
    await client.query(SCHEMA_MIGRATIONS_DDL);
    const { rows } = await client.query<{ id: string }>('SELECT id FROM schema_migrations');
    const applied = new Set(rows.map((r) => r.id));
    const newlyApplied: string[] = [];
    for (const m of MIGRATIONS) {
      if (applied.has(m.id)) continue;
      await client.query(m.sql);
      await client.query('INSERT INTO schema_migrations (id) VALUES ($1)', [m.id]);
      newlyApplied.push(m.id);
    }
    await client.query('COMMIT');
    return newlyApplied;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// CLI entrypoint
if (import.meta.url === `file://${process.argv[1]}`) {
  const log = createLogger({ component: 'migrate' });
  const { databaseUrl } = loadConfig();
  const pool = new pg.Pool({ connectionString: databaseUrl });
  runMigrations(pool)
    .then((applied) => {
      log.info({ applied }, 'migrations applied');
      return pool.end();
    })
    .catch((err) => {
      log.error({ err }, 'migration failed');
      process.exitCode = 1;
      void pool.end();
    });
}
