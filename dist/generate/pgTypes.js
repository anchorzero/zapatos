"use strict";
/*
Zapatos: https://jawj.github.io/zapatos/
Copyright (C) 2020 - 2023 George MacKerron
Released under the MIT licence: see LICENCE file
*/
Object.defineProperty(exports, "__esModule", { value: true });
exports.tsTypeForPgType = void 0;
let warnedAboutInt8AndNumeric = false;
const baseTsTypeForBasePgType = (pgType, enums, context, config) => {
    const hasOwnProp = Object.prototype.hasOwnProperty, warn = config.warningListener === true ? console.log : config.warningListener || (() => void 0);
    function warnAboutLargeNumbers() {
        if (warnedAboutInt8AndNumeric || config.customJSONParsingForLargeNumbers)
            return;
        warn(`Note: this database has bigint/int8 and/or numeric/decimal columns, for which JSON.parse may lose precision. Please read the docs: https://jawj.github.io/zapatos/#custom-json-parsing-for-bigint-and-numeric`);
        warnedAboutInt8AndNumeric = true;
    }
    switch (pgType) {
        case 'money':
            return context === 'JSONSelectable' || context === 'Selectable' ? 'string' :
                '(number | string)';
        // AZ fork: int8 is db.Int8String in *every* context, where upstream gives
        // `number` for JSONSelectable and a `(number | db.Int8String | bigint)` union
        // elsewhere. Our typed-id system (AzId, azIdAsDbInt8String, the Int8String serde
        // hooks in @az/zapatos) is built on int8 always being a string, so upstream's
        // mapping would widen every int8 column in the generated schema and silently
        // reintroduce the precision loss we forked to avoid. No warning here: unlike
        // upstream's default, this mapping cannot lose precision.
        case 'int8':
            return 'db.Int8String';
        // TODO: numeric is our remaining precision hole — we map it to `number`, which
        // JSON.parse will round. Upstream now maps it to db.NumericString (see
        // customJSON.ts / customJSONParsingForLargeNumbers). Adopting that is the right
        // end state but rewrites every numeric column in the generated schema, so it is
        // deliberately left out of the 6.6.1 merge. The warning below is upstream's and
        // is accurate for this case.
        case 'numeric':
            warnAboutLargeNumbers();
            return 'number';
        case 'bytea':
            return context === 'JSONSelectable' ? 'db.ByteArrayString' :
                context === 'Selectable' ? 'Buffer' :
                    '(db.ByteArrayString | Buffer)';
        case 'date':
            return context === 'JSONSelectable' ? 'db.DateString' :
                context === 'Selectable' ? 'Date' :
                    '(db.DateString | Date)';
        case 'timestamp':
            return context === 'JSONSelectable' ? 'db.TimestampString' :
                context === 'Selectable' ? 'Date' :
                    '(db.TimestampString | Date)';
        case 'timestamptz':
            return context === 'JSONSelectable' ? 'db.TimestampTzString' :
                context === 'Selectable' ? 'Date' :
                    '(db.TimestampTzString | Date)';
        case 'time':
            return 'db.TimeString';
        case 'timetz':
            return 'db.TimeTzString';
        case "int4range":
        case "int8range":
        case "numrange":
            return 'db.NumberRangeString';
        case "tsrange": // bounds format depends on pg DateStyle, hence only string-typed
        case "tstzrange": // ditto
        case "daterange": // ditto
            return 'db.DateRangeString';
        case 'interval': // format depends on IntervalStyle, hence only string-typed
        case 'bpchar':
        case 'char':
        case 'varchar':
        case 'text':
        case 'citext':
        case 'uuid':
        case 'inet':
        case 'name':
            return 'string';
        case 'int2':
        case 'int4':
        case 'float4':
        case 'float8':
        case 'oid':
            return 'number';
        case 'bool':
            return 'boolean';
        case 'json':
        case 'jsonb':
            return 'db.JSONValue';
        default:
            if (hasOwnProp.call(enums, pgType))
                return pgType;
            return null;
    }
};
const tsTypeForPgType = (pgType, enums, context, config) => {
    // basic and enum types (enum names can begin with an underscore even if not an array)
    const baseTsType = baseTsTypeForBasePgType(pgType, enums, context, config);
    if (baseTsType !== null)
        return baseTsType;
    // arrays of basic and enum types: pg prefixes these with underscore (_)
    // see https://www.postgresql.org/docs/current/sql-createtype.html#id-1.9.3.94.5.9
    if (pgType.charAt(0) === '_') {
        const arrayTsType = baseTsTypeForBasePgType(pgType.slice(1), enums, context, config);
        if (arrayTsType !== null)
            return arrayTsType + '[]';
    }
    return 'any';
};
exports.tsTypeForPgType = tsTypeForPgType;
