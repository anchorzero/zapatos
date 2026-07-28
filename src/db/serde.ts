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
import {
  ColumnValues,
  NotExactlyOneError,
  ParentColumn,
  SelectResultMode,
  SQL,
  SQLFragment,
} from "./core";

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
      ? // A passthru lateral under `select` aggregates to an array with a null for
        // every parent row the lateral missed; Object.entries(null) would throw a bare
        // TypeError out of the serde walk. Nothing to deserialize, so pass it through.
        values.map<V>((v) => (v == null ? v : applyHookSingle(hook, table, v, lateral)))
      : applyHookSingle(hook, table, values, lateral)
  ) as V;
}

/**
 * A keyed lateral came back NULL. If the sub-query was a `selectExactlyOne`, that
 * is a broken promise — the field is typed non-optional but holds nothing — so we
 * throw, which is what the call site already claims happens.
 *
 * Anything else (`selectOne`, `select`, `count`) may legitimately be absent, and is
 * left alone.
 */
function assertLateralPresent(parentTable: Table, lateralKey: string, subQ: unknown) {
  if (!(subQ instanceof SQLFragment) || subQ.selectResultMode !== SelectResultMode.ExactlyOne) return;

  throw new NotExactlyOneError(
    // A copy, not subQ itself, for two reasons. The sub-query as stored in the
    // `lateral` object has no parentTable — select() sets that on a defensive copy
    // (upstream 6e64759) precisely so the original is not mutated — so compiling
    // the original throws "table alias has no meaning here" the moment it uses
    // db.parent(), making the hint below a lie. And mutating it here would
    // reintroduce the sticky-parent bug that copy() exists to prevent.
    // `instanceof` narrows to SQLFragment<any, any> while NotExactlyOneError.query
    // uses the default generics; Constraint is a phantom field, so the cast only
    // reconciles type parameters.
    (subQ as SQLFragment).copy({ parentTable }),
    `One result expected for lateral '${lateralKey}' on '${parentTable}' but none returned ` +
    '(hint: check `.query.compile()` on this Error)',
  );
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
      const processedExpressions: SQL[] = [];
      for (const expression of v.getExpressions()) {
        if (expression instanceof ColumnValues) {
          const processedExpressionValue = Array.isArray(expression.value)
            ? expression.value.map(
                (x: any) => applyHookSingle(hook, table, { [k]: x })[k]
              )
            : applyHookSingle(hook, table, { [k]: expression.value })[k]; //expression.value
          expression.value = processedExpressionValue;
          processedExpressions.push(expression);
        } else {
          processedExpressions.push(expression);
        }
      }
      v.setExpressions(processedExpressions);
      processed[k as T] = v as any;
      continue;
    }
    const f = hook?.[table]?.[k];
    processed[k as T] = f ? f(v) : v;
  }
  if (lateral) {
    if (lateral instanceof SQLFragment) {
      // TODO: if json/jsonb is removed, we can remove this shim too
      const shim = { rows: [{ result: values }] };
      return lateral.runResultTransform(shim as any);
    } else {
      for (const [k, subQ] of Object.entries(lateral)) {
        const value = processed[k];
        if (value === null || value === undefined) {
          // A lateral is `LEFT JOIN LATERAL ... ON true`, so a missing row is NULL
          // regardless of mode, and the sub-query's own ExactlyOne check never runs
          // (a lateral is never `.run()`). This is the only place that can catch it.
          assertLateralPresent(table, k, subQ);
          continue;  // selectOne / select: an absent row is legal, leave the null in place
        }
        processed[k as T] = applyHook(hook, k as T, value, subQ);
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
    return where;
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
