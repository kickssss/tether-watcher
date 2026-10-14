import { existsSync, mkdirSync } from "fs";
import { dirname } from "path";
import {
  createClient,
  LibsqlError,
  type Client,
  type InArgs,
  type Transaction,
} from "@libsql/client";
import { retry } from "../utils/async";
import { MIGRATIONS } from "./migrations";

/** Startup PRAGMAs (§4.2). Connection-scoped ones are re-applied on every new connection. */
const PRAGMAS = [
  "PRAGMA journal_mode = WAL",
  "PRAGMA synchronous = NORMAL",
  "PRAGMA cache_size = -64000",
  "PRAGMA mmap_size = 268435456",
  "PRAGMA temp_store = MEMORY",
  "PRAGMA foreign_keys = ON",
  "PRAGMA busy_timeout = 5000",
  "PRAGMA wal_autocheckpoint = 4000",
];

/** libsql fails fast with SQLITE_BUSY when two write transactions overlap. */
export function isBusyError(error: unknown): boolean {
  return error instanceof LibsqlError && error.code === "SQLITE_BUSY";
}

/** Any uniqueness/foreign-key violation, so the API can answer 409 instead of 500. */
export function isConstraintError(error: unknown): boolean {
  return (
    error instanceof LibsqlError && error.code.startsWith("SQLITE_CONSTRAINT")
  );
}

function ensureLocalFileDirectory(url: string): void {
  if (!url.startsWith("file:")) return; // remote libsql endpoints need no local directory
  const filePath = decodeURIComponent(
    url.slice("file:".length).replace(/^\/\//, "/"),
  );
  if (filePath.startsWith(":memory:")) return;
  const directory = dirname(filePath);
  if (directory && directory !== "." && !existsSync(directory)) {
    mkdirSync(directory, { recursive: true });
  }
}

/** Anything that can run a statement returning rows (the database or a transaction). */
export interface Queryable {
  query<T>(sql: string, args?: InArgs): Promise<T[]>;
}

/** Transaction facade: `query` returns rows, `run` returns the affected row count. */
export interface Tx extends Queryable {
  run(sql: string, args?: InArgs): Promise<number>;
}

class TransactionFacade implements Tx {
  constructor(private readonly tx: Transaction) {}

  async query<T>(sql: string, args: InArgs = []): Promise<T[]> {
    const result = await this.tx.execute({ sql, args });
    return result.rows as unknown as T[];
  }

  async run(sql: string, args: InArgs = []): Promise<number> {
    const result = await this.tx.execute({ sql, args });
    return result.rowsAffected;
  }
}

/**
 * SQLite access layer (§4.2).
 *
 * Every write is funnelled through one promise queue: libsql's local driver returns
 * SQLITE_BUSY immediately when two write transactions overlap, so the application
 * enforces a single writer. Reads bypass the queue — WAL readers are never blocked.
 */
export class Database implements Queryable {
  private writeQueue: Promise<unknown> = Promise.resolve();

  private constructor(private readonly client: Client) {}

  static async open(url: string): Promise<Database> {
    ensureLocalFileDirectory(url);
    const database = new Database(createClient({ url }));
    for (const pragma of PRAGMAS) {
      await database.client.execute(pragma);
    }
    await database.runMigrations();
    return database;
  }

  async query<T>(sql: string, args: InArgs = []): Promise<T[]> {
    const result = await this.client.execute({ sql, args });
    return result.rows as unknown as T[];
  }

  async queryOne<T>(sql: string, args: InArgs = []): Promise<T | null> {
    const rows = await this.query<T>(sql, args);
    return rows[0] ?? null;
  }

  /**
   * Runs `fn` inside a serialized write transaction. Commits on success; any thrown
   * error — or a crash mid-transaction — rolls the whole block back.
   */
  write<T>(fn: (tx: Tx) => Promise<T>): Promise<T> {
    const run = this.writeQueue.then(
      () => this.runWrite(fn),
      () => this.runWrite(fn),
    );
    this.writeQueue = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private runWrite<T>(fn: (tx: Tx) => Promise<T>): Promise<T> {
    return retry(
      async () => {
        const tx = await this.client.transaction("write");
        try {
          const result = await fn(new TransactionFacade(tx));
          await tx.commit();
          return result;
        } finally {
          // No-op after commit; rolls back when `fn` or the commit threw.
          await tx.close();
        }
      },
      {
        attempts: 5,
        baseDelayMs: 25,
        shouldRetry: isBusyError,
      },
    );
  }

  private async runMigrations(): Promise<void> {
    const row = await this.queryOne<{ user_version: number }>(
      "PRAGMA user_version",
    );
    const currentVersion = Number(row?.user_version ?? 0);
    for (const migration of MIGRATIONS) {
      if (migration.version <= currentVersion) continue;
      await this.write(async (tx) => {
        for (const statement of migration.statements) {
          await tx.run(statement);
        }
        // Integer literal from MIGRATIONS — never user input.
        await tx.run(`PRAGMA user_version = ${migration.version}`);
      });
    }
  }

  /** Flushes queued writes and releases the underlying handle. */
  async close(): Promise<void> {
    await this.writeQueue.catch(() => undefined);
    this.client.close();
  }
}
