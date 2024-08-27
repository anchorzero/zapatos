import { PGlite } from "@electric-sql/pglite";

import { insert, select } from "../src/db";
import { expect, test } from "vitest";
import { PGLiteClient } from "./db";

test("can create a table", async () => {
  const db = new PGlite();

  await db.query("CREATE TABLE foo (bar INT, baz TEXT);");
  const query = insert("foo", { bar: 1, baz: "foobar" }).compile();
  const res = await db.query(query.text, query.values);

  expect(res.affectedRows).toBe(1);
  expect(res.rows).toStrictEqual([{ result: { bar: 1, baz: "foobar" } }]);

  const client = new PGLiteClient(db);

  const xs = await select("foo", {}).run(client);
  expect(xs).toStrictEqual([{ bar: 1, baz: "foobar" }]);
});
