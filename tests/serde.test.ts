import { PGlite } from "@electric-sql/pglite";

import { conditions as dc } from "../src/db";
import { beforeAll, describe, expect, test } from "vitest";
import { PGLiteClient } from "./db";
import { all, sql, vals } from "../src/db/core";
import {
  applyHookForWhere,
  registerSerdeHooksForTable,
  SerdeHook,
} from "../src/db/serde";
import { insert, select } from "../src/db/shortcuts";

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

describe("and/or conditions+serde", () => {
  const db = new PGlite();
  let client: PGLiteClient;

  function nullableSerde<T>({
    deserialize,
    serialize,
    type,
  }: SerdeHook<T>): SerdeHook<T | null> {
    return {
      serialize: serialize
        ? (x: T | null) => (x !== undefined && x != null ? serialize(x) : null)
        : undefined,
      deserialize: deserialize
        ? (x: any) => (x !== undefined && x != null ? deserialize(x) : null)
        : undefined,
      type,
    };
  }

  type ObjType1 = { idName: "ObjType1"; value: BigInt };
  function ObjType1(value: string): ObjType1 {
    return { idName: "ObjType1", value: BigInt(value) };
  }

  type ObjType2 = { idName: "ObjType2"; value: BigInt };
  function ObjType2(value: string): ObjType2 {
    return { idName: "ObjType2", value: BigInt(value) };
  }

  type ObjType3 = { idName: "ObjType3"; value: string };
  function ObjType3(value: string): ObjType3 {
    return { idName: "ObjType3", value };
  }

  const serdeObjType1: SerdeHook<ObjType1> = {
    serialize: (x: ObjType1) => x.value.toString(),
    deserialize: ObjType1,
    type: "ObjType1",
  };

  const serdeObjType2: SerdeHook<ObjType2> = {
    serialize: (x: ObjType2) => x.value.toString(),
    deserialize: ObjType2,
    type: "ObjType2",
  };

  const serdeObjType3: SerdeHook<ObjType3> = {
    serialize: (x: ObjType3) => x.value.toString(),
    deserialize: ObjType3,
    type: "ObjType3",
  };

  const objtype1s = [ObjType1("1"), ObjType1("2"), ObjType1("3")];
  const objtype2s = [ObjType2("100"), ObjType2("200"), ObjType2("300")];
  const objtype3s = [
    ObjType3("00000000-0000-4000-a000-000000000001"),
    ObjType3("00000000-0000-4000-a000-000000000002"),
    ObjType3("00000000-0000-4000-a000-000000000003"),
  ];
  const rows = [
    { pk: 0, obj_type_1: objtype1s[0], obj_type_2: null, obj_type_3: null },
    { pk: 1, obj_type_1: null, obj_type_2: objtype2s[0], obj_type_3: null },
    { pk: 2, obj_type_1: null, obj_type_2: null, obj_type_3: objtype3s[0] },
    {
      pk: 3,
      obj_type_1: objtype1s[1],
      obj_type_2: objtype2s[1],
      obj_type_3: null,
    },
    {
      pk: 4,
      obj_type_1: objtype1s[2],
      obj_type_2: null,
      obj_type_3: objtype3s[1],
    },
    {
      pk: 5,
      obj_type_1: null,
      obj_type_2: objtype2s[2],
      obj_type_3: objtype3s[2],
    },
  ];

  beforeAll(async () => {
    await db.query(
      'CREATE TABLE "test_conditions" (' +
        '"pk" INTEGER NOT NULL,' +
        '"obj_type_1" BIGINT,' +
        '"obj_type_2" BIGINT,' +
        '"obj_type_3" UUID,' +
        'CONSTRAINT "test_conditions_pkey" PRIMARY KEY ("pk"));'
    );

    registerSerdeHooksForTable("test_conditions", {
      obj_type_1: nullableSerde(serdeObjType1),
      obj_type_2: nullableSerde(serdeObjType2),
      obj_type_3: nullableSerde(serdeObjType3),
    });

    client = new PGLiteClient(db);
    await insert("test_conditions", rows).run(client);
  });

  // this is just a sanity check that everything has been set up correctly
  test("basic filter works -- get everything", async () => {
    const results = await select("test_conditions", all, {
      order: { by: "pk", direction: "ASC" },
    }).run(client);
    expect(results).toEqual(rows);
  });

  //another sanity check
  test("basic filter works -- dc.isIn", async () => {
    const results = await select(
      "test_conditions",
      { obj_type_1: dc.isIn(objtype1s) },
      {
        order: { by: "pk", direction: "ASC" },
      }
    ).run(client);
    expect(results).toEqual([rows[0], rows[3], rows[4]]);
  });

  test("dc.and with dc.isNotNull works", async () => {
    const results = await select(
      "test_conditions",
      dc.and({ obj_type_1: dc.isIn(objtype1s) }, { obj_type_1: dc.isNotNull }),
      {
        order: { by: "pk", direction: "ASC" },
      }
    ).run(client);
    expect(results).toEqual([rows[0], rows[3], rows[4]]);
  });

  //TODO we should ensure that this works in mainline zapatos
  test("dc.or with dc.isNotNull works", async () => {
    const results = await select(
      "test_conditions",
      dc.or({ obj_type_1: dc.isIn(objtype1s) }, { obj_type_1: dc.isNull }),
      {
        order: { by: "pk", direction: "ASC" },
      }
    ).run(client);
    console.log(results, rows);
    expect(results).toEqual(rows);
  });

  //TODO we should ensure that this works in mainline zapatos, though per https://github.com/jawj/zapatos/issues/178,
  //     it seems like it should
  test("conditions can be composed", async () => {
    console.log(
      applyHookForWhere(
        "test_conditions",
        dc.and(
          { obj_type_1: objtype1s },
          dc.or({ obj_type_2: objtype2s }, { obj_type_3: objtype3s })
        )
      ).compile()
    );

    /* const results = await select(
      "test_conditions",
      dc.and(
        { obj_type_1: objtype1s },
        dc.or({ obj_type_2: objtype2s }, { obj_type_3: objtype3s })
      ),
      { order: { by: "pk", direction: "ASC" } }
    ).run(client);
    expect(results).toEqual([rows[3], rows[4]]); */
  });

  test("raw sql -- without isTrue works", async () => {
    const results = await select(
      "test_conditions",
      sql`${"obj_type_1"} IN (${vals(
        objtype1s.map((id) => id.value.toString())
      )})`,
      { order: { by: "pk", direction: "ASC" } }
    ).run(client);
    expect(results).toEqual([rows[0], rows[3], rows[4]]);
  });

  test("raw sql -- dc.and with isNotNull fails", async () => {
    const results = await select(
      "test_conditions",
      dc.and(
        sql`${"obj_type_1"} IN (${vals(
          objtype1s.map((id) => id.value.toString())
        )})`,
        { obj_type_1: dc.isNotNull }
      ),
      { order: { by: "pk", direction: "ASC" } }
    ).run(client);
    expect(results).toEqual([rows[0], rows[3], rows[4]]);
  });

  test("raw sql -- dc.or with isNotNull fails", async () => {
    const results = await select(
      "test_conditions",
      dc.or(
        sql`${"obj_type_1"} IN (${vals(
          objtype1s.map((id) => id.value.toString())
        )})`,
        { obj_type_1: dc.isNull }
      ),
      { order: { by: "pk", direction: "ASC" } }
    ).run(client);
    expect(results).toEqual(rows);
  });
});
