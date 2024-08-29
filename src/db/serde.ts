import {
  Column,
  ColumnForTable,
  InsertableForTable,
  SelectableForTable,
  Table,
  Whereable,
  WhereableForTable,
} from "zapatos/schema";
import { type FullLateralOption } from "./shortcuts";
import { ColumnValues, ParentColumn, SQL, SQLFragment } from "./core";

export interface Hook<U, V> {
  [t: Table]: { [c: Column]: (x: U) => V };
}

// TODO: narrow these types
const DESERIALIZE_HOOK: Hook<any, any> = {};
const SERIALIZE_HOOK: Hook<any, any> = {};
export const TYPE_HOOK: { [t: Table]: { [c: Column]: string } } = {};
export const PREAMBLE: string[] = [];

//
var GENERATE_TYPES = false;

type InsertableOrSelectableForTable<T extends Table> =
  | InsertableForTable<T>
  | SelectableForTable<T>;
type InsertableOrSelectableForTableArray<T extends Table> =
  | InsertableForTable<T>[]
  | SelectableForTable<T>[];

function applyHook<
  T extends Table,
  V extends
    | InsertableOrSelectableForTable<T>
    | InsertableOrSelectableForTableArray<T>,
  U,
  W
>(hook: Hook<U, W>, table: Table, values: V, lateral?: FullLateralOption): V {
  return (
    Array.isArray(values)
      ? values.map<V>((v) => applyHookSingle(hook, table, v, lateral))
      : applyHookSingle(hook, table, values, lateral)
  ) as V;
}

function applyHookSQLFragment<T extends Table, U, W>(
  hook: Hook<U, W>,
  table: T,
  v: SQLFragment<any, any>,
  k: string,
  lateral?: FullLateralOption
): SQLFragment<any, any> {
  for (const sql of v.getExpressions()) {
    applyHookSQL(hook, table, sql, k);
  }
  return v;
}

function applyHookSQL<T extends Table, U, W>(
  hook: Hook<U, W>,
  table: T,
  sql: SQL,
  k: string,
  lateral?: FullLateralOption
): void {
  if (sql instanceof ColumnValues) {
    const processedExpressionValue = Array.isArray(sql.value)
      ? sql.value.map((x: any) => applyHookSingle(hook, table, { [k]: x })[k])
      : applyHookSingle(hook, table, { [k]: sql.value })[k]; //expression.value
    sql.value = processedExpressionValue;
  } else if (sql instanceof SQLFragment) {
    applyHookSQLFragment(hook, table, sql, k);
  } else if (Array.isArray(sql)) {
    sql.forEach((subSql) => {
      applyHookSQL(hook, table, subSql, k);
    });
  } else {
    // record type - mapping columns to expressions
    applyHookSingle(hook, table, sql as any);
  }
}

function applyHookSingle<
  T extends Table,
  V extends InsertableOrSelectableForTable<T>,
  U,
  W
>(hook: Hook<U, W>, table: T, values: V, lateral?: FullLateralOption): V {
  const processed: V = {} as V;
  for (const [k, v] of Object.entries(values)) {
    if (v instanceof ParentColumn) {
      processed[k as T] = v as any;
      continue;
    } else if (v instanceof SQLFragment) {
      applyHookSQLFragment(hook, table, v, k, lateral);
      processed[k as T] = v as any;
      continue;
    }
    const f = hook?.[table]?.[k];
    processed[k as T] = f ? (Array.isArray(v) ? v.map(f) : f(v)) : v;
  }
  if (lateral) {
    if (lateral instanceof SQLFragment) {
      // TODO: if json/jsonb is removed, we can remove this shim too
      const shim = { rows: [{ result: values }] };
      return lateral.runResultTransform(shim as any);
    } else {
      for (const [k, subQ] of Object.entries(lateral)) {
        processed[k as T] = processed[k]
          ? applyHook(hook, k as T, processed[k], subQ)
          : processed[k];
      }
    }
  }
  return processed;
}

export function applyHookForWhere<T extends Table, U, W>(
  table: T,
  where: Whereable
) {
  if (where instanceof SQLFragment) {
    return applyHookSQLFragment(SERIALIZE_HOOK, table, where, "sentinel");
  } else {
    return applySerializeHook(table, where);
  }
}

function registerHook<T extends Table, U, V>(
  hook: Hook<U, V>,
  table: T,
  column: ColumnForTable<T>,
  f: (x: U) => V
): void {
  if (!(table in hook)) {
    hook[table] = {};
  }
  hook[table][column] = f;
}

export function applyDeserializeHook<T extends Table>(
  table: T,
  values: SelectableForTable<T> | SelectableForTable<T>[] | undefined,
  lateral?: FullLateralOption
): undefined | SelectableForTable<T> | SelectableForTable<T>[] {
  if (!values) {
    return values;
  }
  return applyHook(DESERIALIZE_HOOK, table, values, lateral);
}

export function applySerializeHook<T extends Table>(
  table: T,
  values: InsertableForTable<T> | InsertableForTable<T>[] | WhereableForTable<T>
): InsertableForTable<T> | InsertableForTable<T>[] | WhereableForTable<T> {
  return applyHook(SERIALIZE_HOOK, table, values);
}

// TODO: f should only read native types
export function registerDeserializeHook<T extends Table, U>(
  table: T,
  column: Column,
  f: (x: any) => U
) {
  registerHook(DESERIALIZE_HOOK, table, column, f);
}

// TODO: f should only return native types
export function registerSerializeHook<T extends Table, U>(
  table: T,
  column: ColumnForTable<T>,
  f: (x: U) => any
) {
  registerHook(SERIALIZE_HOOK, table, column, f);
}

export type SerdeHook<T> = {
  serialize?: (x: T) => any;
  deserialize?: (x: any) => T;
  type?: string;
};

export function registerSerdeHook<T extends Table, U>(
  table: T,
  column: ColumnForTable<T>,
  { serialize, deserialize, type }: SerdeHook<U>
) {
  if (deserialize) {
    registerDeserializeHook(table, column, deserialize);
  }
  if (serialize) {
    registerSerializeHook(table, column, serialize);
  }
  if (type && GENERATE_TYPES) {
    registerTypeHook(table, column, type);
  }
}

type SerdeTableMap<T extends Table> = Partial<
  Record<ColumnForTable<T>, SerdeHook<any>>
>;

function registerTypeHook(table: string, column: string, type: string) {
  if (!(table in TYPE_HOOK)) {
    TYPE_HOOK[table] = {};
  }
  TYPE_HOOK[table][column] = type;
}

export function registerSerdeHooksForTable<T extends Table>(
  table: T,
  map: SerdeTableMap<T>
) {
  for (const [column, serde] of Object.entries(map)) {
    if (serde) {
      registerSerdeHook(table, column, serde);
    }
  }
}

export function setGenerateTypes(flag: boolean) {
  GENERATE_TYPES = flag;
}

export function registerPreamble(str: string) {
  PREAMBLE.push(str);
}
