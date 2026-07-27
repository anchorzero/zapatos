"use strict";
/*
Zapatos: https://jawj.github.io/zapatos/
Copyright (C) 2020 - 2023 George MacKerron
Released under the MIT licence: see LICENCE file
*/
Object.defineProperty(exports, "__esModule", { value: true });
exports.SQLFragment = exports.ParentColumn = exports.ColumnValues = exports.ColumnNames = exports.DangerousRawString = exports.Parameter = exports.toBuffer = exports.all = exports.self = exports.Default = void 0;
exports.strict = strict;
exports.param = param;
exports.raw = raw;
exports.cols = cols;
exports.vals = vals;
exports.parent = parent;
exports.sql = sql;
const config_1 = require("./config");
const utils_1 = require("./utils");
const timing = typeof performance === 'object' ?
    () => performance.now() :
    () => Date.now();
// === symbols, types, wrapper classes and shortcuts ===
/**
 * Compiles to `DEFAULT` for use in `INSERT`/`UPDATE` queries.
 */
exports.Default = Symbol('DEFAULT');
/**
 * Compiles to the current column name within a `Whereable`.
 */
exports.self = Symbol('self');
/**
 * Signals all rows are to be returned (without filtering via a `WHERE` clause)
 */
exports.all = Symbol('all');
/**
 * Make a function `STRICT` in the Postgres sense — where it's an alias for
 * `RETURNS NULL ON NULL INPUT` — with appropriate typing.
 *
 * For example, Zapatos' `toBuffer()` function is defined as:
 *
 * ```
 * export const toBuffer = strict((ba: ByteArrayString) => Buffer.from(ba.slice(2), 'hex'));
 * ```
 *
 * The generic input and output types `FnIn` and `FnOut` can be inferred from
 * `fn`, as seen above, but can also be explicitly narrowed. For example, to
 * convert specifically from `TimestampTzString` to Luxon's `DateTime`, but
 * pass through `null`s unchanged:
 *
 * ```
 * const toDateTime = db.strict<db.TimestampTzString, DateTime>(DateTime.fromISO);
 * ```
 *
 * @param fn The single-argument transformation function to be made strict.
 */
function strict(fn) {
    return function (d) {
        return (d === null ? null : fn(d));
    };
}
/**
 * Convert a `bytea` hex representation to a JavaScript `Buffer`. Note: for
 * large objects, use something like
 * [pg-large-object](https://www.npmjs.com/package/pg-large-object) instead.
 *
 * @param ba The `ByteArrayString` hex representation (or `null`)
 */
exports.toBuffer = strict((ba) => Buffer.from(ba.slice(2), 'hex'));
/**
 * Compiles to a numbered query parameter (`$1`, `$2`, etc) and adds the wrapped value
 * at the appropriate position of the values array passed to `pg`.
 * @param x The value to be wrapped
 * @param cast Optional cast type. If a string, the parameter will be cast to
 * this type within the query e.g. `CAST($1 AS type)` instead of plain `$1`. If
 * `true`, the value will be JSON stringified and cast to `json` (irrespective
 * of the configuration parameters `castArrayParamsToJson` and
 * `castObjectParamsToJson`). If `false`, the value will **not** be JSON-
 * stringified or cast to `json` (again irrespective of the configuration
 * parameters `castArrayParamsToJson` and `castObjectParamsToJson`).
 */
class Parameter {
    constructor(value, cast) {
        this.value = value;
        this.cast = cast;
    }
}
exports.Parameter = Parameter;
/**
 * Returns a `Parameter` instance, which compiles to a numbered query parameter
 * (`$1`, `$2`, etc) and adds its wrapped value at the appropriate position of
 * the values array passed to `pg`.
 * @param x The value to be wrapped
 * @param cast Optional cast type. If a string, the parameter will be cast to
 * this type within the query e.g. `CAST($1 AS type)` instead of plain `$1`. If
 * `true`, the value will be JSON stringified and cast to `json` (irrespective
 * of the configuration parameters `castArrayParamsToJson` and
 * `castObjectParamsToJson`). If `false`, the value will **not** be JSON
 * stringified or cast to `json` (again irrespective of the configuration
 * parameters `castArrayParamsToJson` and `castObjectParamsToJson`).
 */
function param(x, cast) { return new Parameter(x, cast); }
/**
 * 💥💥💣 **DANGEROUS** 💣💥💥
 *
 * Compiles to the wrapped string value, as is, which may enable SQL injection
 * attacks.
 */
class DangerousRawString {
    constructor(value) {
        this.value = value;
    }
}
exports.DangerousRawString = DangerousRawString;
/**
 * 💥💥💣 **DANGEROUS** 💣💥💥
 *
 * Remember [Little Bobby Tables](https://xkcd.com/327/).
 * Did you want `db.param` instead?
 * ---
 * Returns a `DangerousRawString` instance, wrapping a string.
 * `DangerousRawString` compiles to the wrapped string value, as-is, which may
 * enable SQL injection attacks.
 */
function raw(x) { return new DangerousRawString(x); }
/**
 * Wraps either an array or object, and compiles to a quoted, comma-separated
 * list of array values (for use in a `SELECT` query) or object keys (for use
 * in an `INSERT`, `UPDATE` or `UPSERT` query, alongside `ColumnValues`).
 */
class ColumnNames {
    constructor(value) {
        this.value = value;
    }
}
exports.ColumnNames = ColumnNames;
/**
 * Returns a `ColumnNames` instance, wrapping either an array or an object.
 * `ColumnNames` compiles to a quoted, comma-separated list of array values (for
 * use in a `SELECT` query) or object keys (for use in an `INSERT`, `UDPATE` or
 * `UPSERT` query alongside a `ColumnValues`).
 */
function cols(x) { return new ColumnNames(x); }
/**
 * Compiles to a quoted, comma-separated list of object keys for use in an
 * `INSERT`, `UPDATE` or `UPSERT` query, alongside `ColumnNames`.
 */
class ColumnValues {
    constructor(value) {
        this.value = value;
    }
}
exports.ColumnValues = ColumnValues;
/**
 * Returns a ColumnValues instance, wrapping an object. ColumnValues compiles to
 * a quoted, comma-separated list of object keys for use in an INSERT, UPDATE
 * or UPSERT query alongside a `ColumnNames`.
 */
function vals(x) { return new ColumnValues(x); }
/**
 * Compiles to the name of the column it wraps in the table of the parent query.
 * @param value The column name
 */
class ParentColumn {
    constructor(value) {
        this.value = value;
    }
}
exports.ParentColumn = ParentColumn;
/**
 * Returns a `ParentColumn` instance, wrapping a column name, which compiles to
 * that column name of the table of the parent query.
 */
function parent(x) { return new ParentColumn(x); }
// === SQL tagged template strings ===
/**
 * Tagged template function returning a `SQLFragment`. The first generic type
 * argument defines what interpolated value types are allowed. The second
 * defines what type the `SQLFragment` produces, where relevant (i.e. when
 * calling `.run(...)` on it, or using it as the value of an `extras` object).
 */
function sql(literals, ...expressions) {
    return new SQLFragment(Array.prototype.slice.apply(literals), expressions);
}
let preparedNameSeq = 0;
class SQLFragment {
    constructor(literals, expressions) {
        this.literals = literals;
        this.expressions = expressions;
        /**
         * When calling `run`, this function is applied to the object returned by `pg`
         * to produce the result that is returned. By default, the `rows` array is
         * returned — i.e. `(qr) => qr.rows` — but some shortcut functions alter this
         * in order to match their declared `RunResult` type.
         */
        this.runResultTransform = qr => qr.rows;
        this.parentTable = undefined; // used for nested shortcut select queries
        this.preparedName = undefined; // for prepared statements
        this.noop = false; // if true, bypass actually running the query unless forced to e.g. for empty INSERTs
        /**
         * Instruct Postgres to treat this as a prepared statement: see
         * https://node-postgres.com/features/queries#prepared-statements
         * @param name A name for the prepared query. If not specified, it takes the
         * value '_zapatos_prepared_N', where N is an increasing sequence number.
         */
        this.prepared = (name = `_zapatos_prepared_${preparedNameSeq++}`) => {
            this.preparedName = name;
            return this;
        };
        /**
         * Compile and run this query using the provided database connection. What's
         * returned is piped via `runResultTransform` before being returned.
         * @param queryable A database client or pool
         * @param force If true, force this query to hit the DB even if it's marked as a no-op
         */
        this.run = async (queryable, force = false) => {
            var _a;
            const query = this.compile(), { queryListener, resultListener } = (0, config_1.getConfig)(), txnId = (_a = queryable._zapatos) === null || _a === void 0 ? void 0 : _a.txnId;
            if (queryListener)
                queryListener(query, txnId);
            let startMs, result;
            if (resultListener)
                startMs = timing();
            if (!this.noop || force) {
                const qr = await queryable.query(query);
                result = this.runResultTransform(qr);
            }
            else {
                result = this.noopResult;
            }
            if (resultListener)
                resultListener(result, txnId, timing() - startMs, query);
            return result;
        };
        /**
         * Compile this query, returning a `{ text: string, values: any[] }` object
         * that could be passed to the `pg` query function. Arguments are generally
         * only passed when the function calls itself recursively.
         */
        this.compile = (result = { text: '', values: [] }, parentTable, currentColumn) => {
            if (this.parentTable)
                parentTable = this.parentTable;
            if (this.noop)
                result.text += "/* marked no-op: won't hit DB unless forced -> */ ";
            result.text += this.literals[0];
            for (let i = 1, len = this.literals.length; i < len; i++) {
                this.compileExpression(this.expressions[i - 1], result, parentTable, currentColumn);
                result.text += this.literals[i];
            }
            if (this.preparedName != null)
                result.name = this.preparedName;
            return result;
        };
        this.compileExpression = (expression, result = { text: '', values: [] }, parentTable, currentColumn) => {
            var _a;
            if (this.parentTable)
                parentTable = this.parentTable;
            if (expression instanceof SQLFragment) {
                // another SQL fragment? recursively compile this one
                expression.compile(result, parentTable, currentColumn);
            }
            else if (typeof expression === 'string') {
                // if it's a string, it should be a x.Table or x.Column type, so just needs quoting
                result.text += expression.startsWith('"') && expression.endsWith('"') ? expression :
                    `"${expression.replace(/[.]/g, '"."')}"`;
            }
            else if (expression instanceof DangerousRawString) {
                // Little Bobby Tables passes straight through ...
                result.text += expression.value;
            }
            else if (Array.isArray(expression)) {
                // an array's elements are compiled one by one -- note that an empty array can be used as a non-value
                for (let i = 0, len = expression.length; i < len; i++)
                    this.compileExpression(expression[i], result, parentTable, currentColumn);
            }
            else if (expression instanceof Parameter) {
                // parameters become placeholders, and a corresponding entry in the values array
                const placeholder = '$' + String(result.values.length + 1), // 1-based indexing
                config = (0, config_1.getConfig)();
                if (((expression.cast !== false && (expression.cast === true || config.castArrayParamsToJson)) &&
                    Array.isArray(expression.value)) ||
                    ((expression.cast !== false && (expression.cast === true || config.castObjectParamsToJson)) &&
                        (0, utils_1.isPOJO)(expression.value))) {
                    result.values.push(JSON.stringify(expression.value));
                    result.text += `CAST(${placeholder} AS "json")`;
                }
                else if (typeof expression.cast === 'string') {
                    result.values.push(expression.value);
                    result.text += `CAST(${placeholder} AS "${expression.cast}")`;
                }
                else {
                    result.values.push(expression.value);
                    result.text += placeholder;
                }
            }
            else if (expression === exports.Default) {
                // a column default
                result.text += 'DEFAULT';
            }
            else if (expression === exports.self) {
                // alias to the latest column, if applicable
                if (!currentColumn)
                    throw new Error(`The 'self' column alias has no meaning here`);
                this.compileExpression(currentColumn, result);
            }
            else if (expression instanceof ParentColumn) {
                // alias to the parent table (plus optional supplied column name) of a nested query, if applicable
                if (!parentTable)
                    throw new Error(`The 'parent' table alias has no meaning here`);
                this.compileExpression(parentTable, result);
                result.text += '.';
                this.compileExpression((_a = expression.value) !== null && _a !== void 0 ? _a : currentColumn, result);
            }
            else if (expression instanceof ColumnNames) {
                // a ColumnNames-wrapped object -> quoted names in a repeatable order
                // OR a ColumnNames-wrapped array -> quoted array values
                const columnNames = Array.isArray(expression.value) ? expression.value :
                    Object.keys(expression.value).sort();
                for (let i = 0, len = columnNames.length; i < len; i++) {
                    if (i > 0)
                        result.text += ', ';
                    this.compileExpression(String(columnNames[i]), result);
                }
            }
            else if (expression instanceof ColumnValues) {
                // a ColumnValues-wrapped object OR array
                // -> values (in ColumnNames-matching order, if applicable) punted as SQLFragments or Parameters
                if (Array.isArray(expression.value)) {
                    const values = expression.value;
                    for (let i = 0, len = values.length; i < len; i++) {
                        const value = values[i];
                        if (i > 0)
                            result.text += ', ';
                        if (value instanceof SQLFragment)
                            this.compileExpression(value, result, parentTable);
                        else
                            this.compileExpression(new Parameter(value), result, parentTable);
                    }
                }
                else {
                    const columnNames = Object.keys(expression.value).sort(), columnValues = columnNames.map(k => expression.value[k]);
                    for (let i = 0, len = columnValues.length; i < len; i++) {
                        const columnName = columnNames[i], columnValue = columnValues[i];
                        if (i > 0)
                            result.text += ', ';
                        if (columnValue instanceof SQLFragment ||
                            columnValue instanceof Parameter ||
                            columnValue === exports.Default)
                            this.compileExpression(columnValue, result, parentTable, columnName);
                        else
                            this.compileExpression(new Parameter(columnValue), result, parentTable, columnName);
                    }
                }
            }
            else if (typeof expression === 'object') {
                if (expression === globalThis)
                    throw new Error('Did you use `self` (the global object) where you meant `db.self` (the Zapatos value)? The global object cannot be embedded in a query.');
                // must be a Whereable object, so put together a WHERE clause
                const columnNames = Object.keys(expression).sort();
                if (columnNames.length) { // if the object is not empty
                    result.text += '(';
                    for (let i = 0, len = columnNames.length; i < len; i++) {
                        const columnName = columnNames[i], columnValue = expression[columnName];
                        if (i > 0)
                            result.text += ' AND ';
                        if (columnValue instanceof SQLFragment) {
                            result.text += '(';
                            this.compileExpression(columnValue, result, parentTable, columnName);
                            result.text += ')';
                        }
                        else {
                            this.compileExpression(columnName, result);
                            result.text += ` = `;
                            this.compileExpression(columnValue instanceof ParentColumn ? columnValue : new Parameter(columnValue), result, parentTable, columnName);
                        }
                    }
                    result.text += ')';
                }
                else {
                    // or if it is empty, it should always match
                    result.text += 'TRUE';
                }
            }
            else {
                throw new Error(`Alien object while interpolating SQL: ${expression}`);
            }
        };
    }
    getExpressions() {
        return this.expressions;
    }
    setExpressions(expressions) {
        this.expressions = expressions;
    }
    /**
     * Performs a shallow copy of this SQLFragment, optionally overriding some of its properties.
     * @param override The properties to override
     */
    copy(override) {
        const { literals = this.literals, expressions = this.expressions, ...overrideRest } = override !== null && override !== void 0 ? override : {};
        const copy = new SQLFragment(literals, expressions);
        return Object.assign(copy, {
            parentTable: this.parentTable,
            preparedName: this.preparedName,
            noop: this.noop,
            noopResult: this.noopResult
        }, overrideRest);
    }
}
exports.SQLFragment = SQLFragment;
