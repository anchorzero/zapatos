import type * as pg from "pg";
import { PGlite } from "@electric-sql/pglite";
import { SQLFragment } from "../db";
import { NoticeMessage } from "pg-protocol/dist/messages";
import { Writable, Readable } from "stream";

export class PGLiteClient implements pg.ClientBase {
  private readonly db: PGlite;
  constructor(db: PGlite) {
    this.db = db;
  }
  query<T extends pg.Submittable>(queryStream: T): T;
  query<R extends any[] = any[], I extends any[] = any[]>(
    queryConfig: pg.QueryArrayConfig<I>,
    values?: I
  ): Promise<pg.QueryArrayResult<R>>;
  query<R extends pg.QueryResultRow = any, I extends any[] = any[]>(
    queryConfig: pg.QueryConfig<I>
  ): Promise<pg.QueryResult<R>>;
  query<R extends pg.QueryResultRow = any, I extends any[] = any[]>(
    queryTextOrConfig: string | pg.QueryConfig<I>,
    values?: I
  ): Promise<pg.QueryResult<R>>;
  query<R extends any[] = any[], I extends any[] = any[]>(
    queryConfig: pg.QueryArrayConfig<I>,
    callback: (err: Error, result: pg.QueryArrayResult<R>) => void
  ): void;
  query<R extends pg.QueryResultRow = any, I extends any[] = any[]>(
    queryTextOrConfig: string | pg.QueryConfig<I>,
    callback: (err: Error, result: pg.QueryResult<R>) => void
  ): void;
  query<R extends pg.QueryResultRow = any, I extends any[] = any[]>(
    queryText: string,
    values: any[],
    callback: (err: Error, result: pg.QueryResult<R>) => void
  ): void;
  query<T extends pg.Submittable, R extends any[]>(
    query: unknown,
    _values?: unknown,
    callback?: unknown
  ):
    | void
    | T
    | Promise<pg.QueryArrayResult<R>>
    | Promise<pg.QueryResult<R>>
    | Promise<pg.QueryResult<R>> {
    if (!("text" in (query as pg.QueryConfig))) {
      throw new Error("Method not implemented.");
    }
    const { text, values } = query as pg.QueryConfig;
    return this.db.query(text as string, values as any[]).then((x) => {
      return {
        rowCount: x.rows.length,
        rows: x.rows as R[],
        command: text as string,
        oid: 0,
        fields: [],
      };
    });
  }

  connect(): Promise<void>;
  connect(callback: (err: Error) => void): void;
  connect(callback?: unknown): void | Promise<void> {
    throw new Error("Method not implemented.");
  }

  copyFrom(queryText: string): Writable {
    throw new Error("Method not implemented.");
  }
  copyTo(queryText: string): Readable {
    throw new Error("Method not implemented.");
  }
  pauseDrain(): void {
    throw new Error("Method not implemented.");
  }
  resumeDrain(): void {
    throw new Error("Method not implemented.");
  }
  escapeIdentifier(str: string): string {
    throw new Error("Method not implemented.");
  }
  escapeLiteral(str: string): string {
    throw new Error("Method not implemented.");
  }
  on(event: "drain", listener: () => void): this;
  on(event: "error", listener: (err: Error) => void): this;
  on(event: "notice", listener: (notice: NoticeMessage) => void): this;
  on(event: "notification", listener: (message: pg.Notification) => void): this;
  on(event: "end", listener: () => void): this;
  on(event: unknown, listener: unknown): this {
    throw new Error("Method not implemented.");
  }
  addListener(
    eventName: string | symbol,
    listener: (...args: any[]) => void
  ): this {
    throw new Error("Method not implemented.");
  }
  once(eventName: string | symbol, listener: (...args: any[]) => void): this {
    throw new Error("Method not implemented.");
  }
  removeListener(
    eventName: string | symbol,
    listener: (...args: any[]) => void
  ): this {
    throw new Error("Method not implemented.");
  }
  off(eventName: string | symbol, listener: (...args: any[]) => void): this {
    throw new Error("Method not implemented.");
  }
  removeAllListeners(event?: string | symbol): this {
    throw new Error("Method not implemented.");
  }
  setMaxListeners(n: number): this {
    throw new Error("Method not implemented.");
  }
  getMaxListeners(): number {
    throw new Error("Method not implemented.");
  }
  listeners(eventName: string | symbol): Function[] {
    throw new Error("Method not implemented.");
  }
  rawListeners(eventName: string | symbol): Function[] {
    throw new Error("Method not implemented.");
  }
  emit(eventName: string | symbol, ...args: any[]): boolean {
    throw new Error("Method not implemented.");
  }
  listenerCount(eventName: string | symbol): number {
    throw new Error("Method not implemented.");
  }
  prependListener(
    eventName: string | symbol,
    listener: (...args: any[]) => void
  ): this {
    throw new Error("Method not implemented.");
  }
  prependOnceListener(
    eventName: string | symbol,
    listener: (...args: any[]) => void
  ): this {
    throw new Error("Method not implemented.");
  }
  eventNames(): Array<string | symbol> {
    throw new Error("Method not implemented.");
  }
}

export async function executeQuery(db: PGlite, sql: SQLFragment) {
  const query = await sql.compile();
  await db.query(query.text, query.values);
}
