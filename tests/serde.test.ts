import { PGlite } from "@electric-sql/pglite";

import {
  insert,
  registerSerdeHooksForTable,
  select,
  conditions as dc,
} from "../src/db";
import type { SerdeHook } from "../src/db";
import { expect, test } from "vitest";
import { PGLiteClient } from "./db";

test("can insert and select with serde", async () => {
  const db = new PGlite();

  await db.query("CREATE TABLE foo (bar INT, baz TEXT, date TIMESTAMPTZ);");

  const dateSerde: SerdeHook<Date> = {
    serialize: (x: Date) => x.toISOString(),
    deserialize: (x: string) => new Date(x),
    type: "Date",
  };

  registerSerdeHooksForTable("foo", {
    date: dateSerde,
  });

  const date = new Date("2024-08-27T11:52:06.626+00:00");

  const query = insert("foo", { bar: 1, baz: "foobar", date }).compile();
  const res = await db.query(query.text, query.values);

  expect(res.affectedRows).toBe(1);
  expect(res.rows).toStrictEqual([
    {
      result: { bar: 1, baz: "foobar", date: "2024-08-27T11:52:06.626+00:00" },
    },
  ]);

  const client = new PGLiteClient(db);

  const xs = await select("foo", {}).run(client);
  expect(xs).toStrictEqual([{ bar: 1, baz: "foobar", date }]);
});

test("can use conditions with serde", async () => {
  const db = new PGlite();

  await db.query("CREATE TABLE foo (bar INT, baz TEXT, date TIMESTAMPTZ);");

  const dateSerde: SerdeHook<Date> = {
    serialize: (x: Date) => x.toISOString(),
    deserialize: (x: string) => new Date(x),
    type: "Date",
  };

  registerSerdeHooksForTable("foo", {
    date: dateSerde,
  });

  const date1 = new Date("2024-08-27T00:00:00.000+00:00");
  const date2 = new Date("2024-08-30T00:00:00.000+00:00");
  const date3 = new Date("2024-08-31T00:00:00.000+00:00");

  const client = new PGLiteClient(db);

  await insert("foo", [
    { bar: 1, baz: "foobar", date: date1 },
    { bar: 2, baz: "racecar", date: date2 },
    { bar: 3, baz: "sailboat", date: date3 },
  ]).run(client);

  let xs = await select("foo", {}).run(client);
  expect(xs).toStrictEqual([
    { bar: 1, baz: "foobar", date: date1 },
    { bar: 2, baz: "racecar", date: date2 },
    { bar: 3, baz: "sailboat", date: date3 },
  ]);

  // less than works
  xs = await select("foo", { bar: dc.lt(2) }).run(client);
  expect(xs).toStrictEqual([{ bar: 1, baz: "foobar", date: date1 }]);

  // OR works, less than works, greater than works
  xs = await select("foo", { bar: dc.or(dc.lt(2), dc.gt(2)) }).run(client);
  expect(xs).toStrictEqual([
    { bar: 1, baz: "foobar", date: date1 },
    { bar: 3, baz: "sailboat", date: date3 },
  ]);

  // serde less than works
  xs = await select("foo", { date: dc.lt(date2) }).run(client);
  expect(xs).toStrictEqual([{ bar: 1, baz: "foobar", date: date1 }]);

  // serde OR works, less than works, greater than works
  xs = await select("foo", { date: dc.or(dc.lt(date2), dc.gt(date2)) }).run(
    client
  );
  expect(xs).toStrictEqual([
    { bar: 1, baz: "foobar", date: date1 },
    { bar: 3, baz: "sailboat", date: date3 },
  ]);
});
