"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PREAMBLE = exports.TYPE_HOOK = void 0;
exports.applyHookForWhere = applyHookForWhere;
exports.applyDeserializeHook = applyDeserializeHook;
exports.applySerializeHook = applySerializeHook;
exports.registerDeserializeHook = registerDeserializeHook;
exports.registerSerializeHook = registerSerializeHook;
exports.registerSerdeHook = registerSerdeHook;
exports.registerSerdeHooksForTable = registerSerdeHooksForTable;
exports.setGenerateTypes = setGenerateTypes;
exports.registerPreamble = registerPreamble;
const core_1 = require("./core");
// TODO: narrow these types
const DESERIALIZE_HOOK = {};
const SERIALIZE_HOOK = {};
exports.TYPE_HOOK = {};
exports.PREAMBLE = [];
//
var GENERATE_TYPES = false;
function applyHook(hook, table, values, lateral) {
    return (Array.isArray(values)
        ? // A passthru lateral under `select` aggregates to an array with a null for
            // every parent row the lateral missed; Object.entries(null) would throw a bare
            // TypeError out of the serde walk. Nothing to deserialize, so pass it through.
            values.map((v) => (v == null ? v : applyHookSingle(hook, table, v, lateral)))
        : applyHookSingle(hook, table, values, lateral));
}
/**
 * A keyed lateral came back NULL. If the sub-query was a `selectExactlyOne`, that
 * is a broken promise — the field is typed non-optional but holds nothing — so we
 * throw, which is what the call site already claims happens.
 *
 * Anything else (`selectOne`, `select`, `count`) may legitimately be absent, and is
 * left alone.
 */
function assertLateralPresent(parentTable, lateralKey, subQ) {
    if (!(subQ instanceof core_1.SQLFragment) || subQ.selectResultMode !== core_1.SelectResultMode.ExactlyOne)
        return;
    throw new core_1.NotExactlyOneError(
    // A copy, not subQ itself, for two reasons. The sub-query as stored in the
    // `lateral` object has no parentTable — select() sets that on a defensive copy
    // (upstream 6e64759) precisely so the original is not mutated — so compiling
    // the original throws "table alias has no meaning here" the moment it uses
    // db.parent(), making the hint below a lie. And mutating it here would
    // reintroduce the sticky-parent bug that copy() exists to prevent.
    // `instanceof` narrows to SQLFragment<any, any> while NotExactlyOneError.query
    // uses the default generics; Constraint is a phantom field, so the cast only
    // reconciles type parameters.
    subQ.copy({ parentTable }), `One result expected for lateral '${lateralKey}' on '${parentTable}' but none returned ` +
        '(hint: check `.query.compile()` on this Error)');
}
function applyHookSingle(hook, table, values, lateral) {
    var _a;
    const processed = {};
    for (const [k, v] of Object.entries(values)) {
        if (v instanceof core_1.ParentColumn) {
            processed[k] = v;
            continue;
        }
        else if (v instanceof core_1.SQLFragment) {
            const processedExpressions = [];
            for (const expression of v.getExpressions()) {
                if (expression instanceof core_1.ColumnValues) {
                    const processedExpressionValue = Array.isArray(expression.value)
                        ? expression.value.map((x) => applyHookSingle(hook, table, { [k]: x })[k])
                        : applyHookSingle(hook, table, { [k]: expression.value })[k]; //expression.value
                    expression.value = processedExpressionValue;
                    processedExpressions.push(expression);
                }
                else {
                    processedExpressions.push(expression);
                }
            }
            v.setExpressions(processedExpressions);
            processed[k] = v;
            continue;
        }
        const f = (_a = hook === null || hook === void 0 ? void 0 : hook[table]) === null || _a === void 0 ? void 0 : _a[k];
        processed[k] = f ? f(v) : v;
    }
    if (lateral) {
        if (lateral instanceof core_1.SQLFragment) {
            // TODO: if json/jsonb is removed, we can remove this shim too
            const shim = { rows: [{ result: values }] };
            return lateral.runResultTransform(shim);
        }
        else {
            for (const [k, subQ] of Object.entries(lateral)) {
                const value = processed[k];
                if (value === null || value === undefined) {
                    // A lateral is `LEFT JOIN LATERAL ... ON true`, so a missing row is NULL
                    // regardless of mode, and the sub-query's own ExactlyOne check never runs
                    // (a lateral is never `.run()`). This is the only place that can catch it.
                    assertLateralPresent(table, k, subQ);
                    continue; // selectOne / select: an absent row is legal, leave the null in place
                }
                processed[k] = applyHook(hook, k, value, subQ);
            }
        }
    }
    return processed;
}
function applyHookForWhere(table, where) {
    if (where instanceof core_1.SQLFragment) {
        return where;
    }
    else {
        return applySerializeHook(table, where);
    }
}
function registerHook(hook, table, column, f) {
    if (!(table in hook)) {
        hook[table] = {};
    }
    hook[table][column] = f;
}
function applyDeserializeHook(table, values, lateral) {
    if (!values) {
        return values;
    }
    return applyHook(DESERIALIZE_HOOK, table, values, lateral);
}
function applySerializeHook(table, values) {
    return applyHook(SERIALIZE_HOOK, table, values);
}
// TODO: f should only read native types
function registerDeserializeHook(table, column, f) {
    registerHook(DESERIALIZE_HOOK, table, column, f);
}
// TODO: f should only return native types
function registerSerializeHook(table, column, f) {
    registerHook(SERIALIZE_HOOK, table, column, f);
}
function registerSerdeHook(table, column, { serialize, deserialize, type }) {
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
function registerTypeHook(table, column, type) {
    if (!(table in exports.TYPE_HOOK)) {
        exports.TYPE_HOOK[table] = {};
    }
    exports.TYPE_HOOK[table][column] = type;
}
function registerSerdeHooksForTable(table, map) {
    for (const [column, serde] of Object.entries(map)) {
        if (serde) {
            registerSerdeHook(table, column, serde);
        }
    }
}
function setGenerateTypes(flag) {
    GENERATE_TYPES = flag;
}
function registerPreamble(str) {
    exports.PREAMBLE.push(str);
}
