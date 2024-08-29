import { PGlite } from "@electric-sql/pglite";

import { conditions as dc } from "../src/db";
import { beforeAll, describe, expect, test } from "vitest";
import { PGLiteClient } from "./db";
import { all, sql, vals, self as zself } from "../src/db/core";
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

  const rows2 = [
    { pk: 0, plain: [] },
    { pk: 1, plain: [1] },
    { pk: 2, plain: [1, 1] },
    { pk: 3, plain: [1, 1, 2] },
    { pk: 4, plain: [1, 1, 2, 3] },
    { pk: 5, plain: [1, 1, 2, 3, 5] },
    { pk: 6, plain: [1, 1, 2, 3, 5, 8] },
    { pk: 7, plain: [1, 1, 2, 3, 5, 8, 13] },
    { pk: 8, plain: [1, 1, 2, 3, 5, 8, 13, 21] },
  ]

  beforeAll(async () => {
    await db.query(
      'CREATE TABLE "test_conditions" (' +
        '"pk" INTEGER NOT NULL,' +
        '"obj_type_1" BIGINT,' +
        '"obj_type_2" BIGINT,' +
        '"obj_type_3" UUID,' +
        'CONSTRAINT "test_conditions_pkey" PRIMARY KEY ("pk"));'
    );

    await db.query(
      'CREATE TABLE "test_conditions_array" (' +
        '"pk" INTEGER NOT NULL,' +
        '"plain" INT array,' +
        // I don't know if the normal serde hook will even work with this
        //TODO once we verify the plain field works with sunil's addition,
        //     can choose if we want to add tests for serde hook with this array
        //'"obj_type_1" BIGINT array,' +
        'CONSTRAINT "test_conditions_array_pkey" PRIMARY KEY ("pk"));'
    );

    registerSerdeHooksForTable("test_conditions", {
      obj_type_1: nullableSerde(serdeObjType1),
      obj_type_2: nullableSerde(serdeObjType2),
      obj_type_3: nullableSerde(serdeObjType3),
    });

    client = new PGLiteClient(db);
    await insert("test_conditions", rows).run(client);
    //TODO maybe we should split this out into a separate describe
    await insert("test_conditions_array", rows2).run(client);
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
    expect(results).toEqual(rows);
  });

  test("conditions can be composed", async () => {
    const results = await select(
      "test_conditions",
      dc.and(
        { obj_type_1: dc.isIn(objtype1s) },
        dc.or(
          { obj_type_2: dc.isIn(objtype2s) },
          { obj_type_3: dc.isIn(objtype3s) }
        )
      ),
      { order: { by: "pk", direction: "ASC" } }
    ).run(client);
    expect(results).toEqual([rows[3], rows[4]]);
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

  test("raw sql -- a raw sql TRUE", async () => {
    const results = await select("test_conditions", sql`TRUE`, { order: { by: "pk", direction: "ASC" } }).run(client);
    expect(results).toEqual(rows);
  })

  test("raw sql -- a raw sql TRUE with an and", async () => {
    const results = await select("test_conditions", dc.or({ object_type_1: dc.isIn(objtype1s) }, sql`TRUE`), { order: { by: "pk", direction: "ASC" } }).run(client);
    expect(results).toEqual(rows);
  })

  test("mixing nested sql and conditions works -- basic", async () => {
    const or =
      dc.or(dc.or(
        sql`${"obj_type_1"} IN (${vals(
          objtype1s.map((id) => id.value.toString())
        )})`,
        { obj_type_1: dc.isNotIn(objtype1s) }
      ), { obj_type_1: dc.isNull });
    const results = await select("test_conditions", or, { order: { by: "pk", direction: "ASC" } }).run(client);
    expect(results).toEqual(rows);
  });

  const baseOr = dc.or(
    dc.or(
      sql`${"obj_type_1"} IN (${vals(
        objtype1s.map((id) => id.value.toString())
      )})`,
      { obj_type_1: dc.isNotIn(objtype1s) },
      dc.or(
        sql`${"obj_type_3"} IN (${vals(
          objtype3s.map((id) => id.value.toString())
        )})`,
        { obj_type_3: dc.isNotIn(objtype3s) }
      )
    ),
    dc.or(
      sql`${"obj_type_2"} IN (${vals(
        objtype2s.map((id) => id.value.toString())
      )})`,
      { obj_type_2: dc.isNotIn(objtype2s) }
    )
  );

  test("mixing nested sql and conditions works -- more complex", async () => {
    const results = await select("test_conditions", baseOr, { order: { by: "pk", direction: "ASC" } }).run(client);
    expect(results).toEqual(rows);
  });

  test("mixing nested sql and conditions works -- very nested", async () => {
    const nestedOr = dc.or(dc.and(baseOr, baseOr, sql`TRUE`), sql`FALSE`, dc.and(baseOr, baseOr, sql`TRUE`));
    const results = await select("test_conditions", nestedOr, { order: { by: "pk", direction: "ASC" } }).run(client);
    expect(results).toEqual(rows);
  });

  test("wherable with no hook and sql using self", async () => {
    const results = await select("test_conditions", { pk: sql`${zself} = 1`}, { order: { by: "pk", direction: "ASC" } }).run(client);
    expect(results).toEqual([rows[1]]);
  })

  test("use wherable with sql using self and hook", async () => {
    const results = await select("test_conditions", { obj_type_1: sql`${zself} = 1`}, { order: { by: "pk", direction: "ASC" } }).run(client);
    expect(results).toEqual([rows[0]]);
  })

  test("use wherable with sql using self and hook with values", async () => {
    const results = await select("test_conditions", { obj_type_1: sql`${zself} IN (${vals([1,2,3])})`}, { order: { by: "pk", direction: "ASC" } }).run(client);
    expect(results).toEqual([rows[0], rows[3], rows[4]]);
  })

  test("use wherable with sql using self, hook, and conditions", async () => {
    const results = await select("test_conditions", dc.and({ obj_type_1: dc.isIn(objtype1s) }, { obj_type_2: sql`${zself} = 100`}), { order: { by: "pk", direction: "ASC" } }).run(client);
    expect(results).toEqual(rows[3]);
  })

  test("conditions with an array field -- basic sql true", async () => {
    const results = await select("test_conditions_array", sql`TRUE`, { order: { by: "pk", direction: "ASC" } }).run(client);
    expect(results).toEqual(rows2);
  });

  test("conditions with an array field -- basic condition", async () => {
    const results = await select("test_conditions_array", { pk: dc.between(3, 5) }, { order: { by: "pk", direction: "ASC" } }).run(client);
    expect(results).toEqual(rows2.slice(3, 6));
  });

  test("conditions with an array field -- basic condition with and", async () => {
    const results = await select("test_conditions_array", dc.and({ pk: dc.between(2, 6) }, { pk: dc.between(1, 5) }), { order: { by: "pk", direction: "ASC" } }).run(client);
    expect(results).toEqual(rows2.slice(2, 6));
  });

  test("conditions with an array field -- basic condition with array field", async () => {
    const results = await select("test_conditions_array", dc.and({ pk: dc.between(2, 6) }, { plain: dc.isNotNull }), { order: { by: "pk", direction: "ASC" } }).run(client);
    expect(results).toEqual(rows2.slice(2, 7));
  });

  test("conditions with an array field -- basic condition with array field condition", async () => {
    const results = await select("test_conditions_array", dc.and({ pk: dc.between(2, 7) }, { plain: dc.isIn([[], [1, 1, 2], [1, 1, 2, 3, 5]]) }), { order: { by: "pk", direction: "ASC" } }).run(client);
    expect(results).toEqual([rows2[3], rows2[5]]);
  });

  //TODO spend a bit more time with sunil's hook and try and invoke the Array.isArray path that
  //     there was some worry about
});
