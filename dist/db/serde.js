"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.registerPreamble = exports.setGenerateTypes = exports.registerSerdeHooksForTable = exports.registerSerdeHook = exports.registerSerializeHook = exports.registerDeserializeHook = exports.applySerializeHook = exports.applyDeserializeHook = exports.applyHookForWhere = exports.PREAMBLE = exports.TYPE_HOOK = void 0;
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
        ? values.map((v) => applyHookSingle(hook, table, v, lateral))
        : applyHookSingle(hook, table, values, lateral));
}
function applyHookSQLFragment(hook, table, v, k, lateral) {
    for (const sql of v.getExpressions()) {
        applyHookSQL(hook, table, sql, k);
    }
    return v;
}
function applyHookSQL(hook, table, sql, k, lateral) {
    if (sql instanceof core_1.ColumnValues) {
        const processedExpressionValue = Array.isArray(sql.value)
            ? sql.value.map((x) => applyHookSingle(hook, table, { [k]: x })[k])
            : applyHookSingle(hook, table, { [k]: sql.value })[k]; //expression.value
        sql.value = processedExpressionValue;
    }
    else if (sql instanceof core_1.SQLFragment) {
        applyHookSQLFragment(hook, table, sql, k);
    }
    else if (Array.isArray(sql)) {
        sql.forEach((subSql) => {
            applyHookSQL(hook, table, subSql, k);
        });
    }
    else {
        // record type - mapping columns to expressions
        applyHookSingle(hook, table, sql);
    }
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
            applyHookSQLFragment(hook, table, v, k, lateral);
            processed[k] = v;
            continue;
        }
        const f = (_a = hook === null || hook === void 0 ? void 0 : hook[table]) === null || _a === void 0 ? void 0 : _a[k];
        processed[k] = f ? (Array.isArray(v) ? v.map(f) : f(v)) : v;
    }
    if (lateral) {
        if (lateral instanceof core_1.SQLFragment) {
            // TODO: if json/jsonb is removed, we can remove this shim too
            const shim = { rows: [{ result: values }] };
            return lateral.runResultTransform(shim);
        }
        else {
            for (const [k, subQ] of Object.entries(lateral)) {
                processed[k] = processed[k]
                    ? applyHook(hook, k, processed[k], subQ)
                    : processed[k];
            }
        }
    }
    return processed;
}
function applyHookForWhere(table, where) {
    if (where instanceof core_1.SQLFragment) {
        return applyHookSQLFragment(SERIALIZE_HOOK, table, where, "sentinel");
    }
    else {
        return applySerializeHook(table, where);
    }
}
exports.applyHookForWhere = applyHookForWhere;
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
exports.applyDeserializeHook = applyDeserializeHook;
function applySerializeHook(table, values) {
    return applyHook(SERIALIZE_HOOK, table, values);
}
exports.applySerializeHook = applySerializeHook;
// TODO: f should only read native types
function registerDeserializeHook(table, column, f) {
    registerHook(DESERIALIZE_HOOK, table, column, f);
}
exports.registerDeserializeHook = registerDeserializeHook;
// TODO: f should only return native types
function registerSerializeHook(table, column, f) {
    registerHook(SERIALIZE_HOOK, table, column, f);
}
exports.registerSerializeHook = registerSerializeHook;
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
exports.registerSerdeHook = registerSerdeHook;
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
exports.registerSerdeHooksForTable = registerSerdeHooksForTable;
function setGenerateTypes(flag) {
    GENERATE_TYPES = flag;
}
exports.setGenerateTypes = setGenerateTypes;
function registerPreamble(str) {
    exports.PREAMBLE.push(str);
}
exports.registerPreamble = registerPreamble;
