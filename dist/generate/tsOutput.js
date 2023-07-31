"use strict";
/*
Zapatos: https://jawj.github.io/zapatos/
Copyright (C) 2020 - 2022 George MacKerron
Released under the MIT licence: see LICENCE file
*/
Object.defineProperty(exports, "__esModule", { value: true });
exports.tsForConfig = void 0;
const pg = require("pg");
const enums_1 = require("./enums");
const header_1 = require("./header");
const tables_1 = require("./tables");
const canaryVersion = 104, versionCanary = `
// got a type error on schemaVersionCanary below? update by running \`npx zapatos\`
export interface schemaVersionCanary extends db.SchemaVersionCanary { version: ${canaryVersion} }
`;
const declareModule = (module, declarations) => `
declare module '${module}' {
${declarations.replace(/^(?=[ \t]*\S)/gm, '  ')}
}
`;
const customTypeHeader = `/*
** Please edit this file as needed **
It's been generated by Zapatos as a custom type definition placeholder, and won't be overwritten
*/
`;
const sourceFilesForCustomTypes = (customTypes) => Object.fromEntries(Object.entries(customTypes)
    .map(([name, baseType]) => [
    name,
    customTypeHeader + declareModule('zapatos/custom', (baseType === 'db.JSONValue' ? `import type * as db from 'zapatos/db';\n` : ``) +
        `export type ${name} = ${baseType};  // replace with your custom type or interface as desired`)
]));
function indentAll(level, s) {
    if (level === 0)
        return s;
    return s.replace(/^/gm, ' '.repeat(level));
}
const tsForConfig = async (config, debug) => {
    var _a;
    let querySeq = 0;
    const { schemas, db } = config, pool = new pg.Pool(db), queryFn = async (query, seq = querySeq++) => {
        try {
            debug(`>>> query ${seq} >>>\n${query.text.replace(/^\s+|\s+$/mg, '')}\n+ ${JSON.stringify(query.values)}\n`);
            const result = await pool.query(query);
            debug(`<<< result ${seq} <<<\n${JSON.stringify(result, null, 2)}\n`);
            return result;
        }
        catch (e) {
            console.log(`*** error ${seq} ***`, e);
            process.exit(1);
        }
    }, customTypes = {}, schemaNames = Object.keys(schemas), schemaData = (await Promise.all(schemaNames.map(async (schema) => {
        const rules = schemas[schema], tables = rules.exclude === '*' ? [] : // exclude takes precedence
            (await (0, tables_1.relationsInSchema)(schema, queryFn))
                .filter(rel => rules.include === '*' || rules.include.indexOf(rel.name) >= 0)
                .filter(rel => rules.exclude.indexOf(rel.name) < 0), enums = await (0, enums_1.enumDataForSchema)(schema, queryFn), tableDefs = await Promise.all(tables.map(async (table) => (0, tables_1.definitionForRelationInSchema)(table, schema, enums, customTypes, config, queryFn))), schemaIsUnprefixed = schema === config.unprefixedSchema, none = '/* (none) */', schemaDef = `/* === schema: ${schema} === */\n` +
            (schemaIsUnprefixed ? '' : `\nexport namespace ${schema} {\n`) +
            indentAll(schemaIsUnprefixed ? 0 : 2, `\n/* --- enums --- */\n` +
                ((0, enums_1.enumTypesForEnumData)(enums) || none) +
                `\n\n/* --- tables --- */\n` +
                (tableDefs.join('\n') || none) +
                `\n\n/* --- aggregate types --- */\n` +
                (schemaIsUnprefixed ?
                    `\nexport namespace ${schema} {` + (indentAll(2, (0, tables_1.crossTableTypesForTables)(tables) || none)) + '\n}\n' :
                    ((0, tables_1.crossTableTypesForTables)(tables) || none))) + '\n' +
            (schemaIsUnprefixed ? '' : `}\n`);
        return { schemaDef, tables };
    }))), schemaDefs = schemaData.map(r => r.schemaDef), schemaTables = schemaData.map(r => r.tables), allTables = [].concat(...schemaTables), hasCustomTypes = Object.keys(customTypes).length > 0, ts = (0, header_1.header)() + declareModule('zapatos/schema', `\nimport type * as db from 'zapatos/db';\n` +
        (hasCustomTypes ? `import type * as c from 'zapatos/custom';\n` : ``) +
        ((_a = config.preamble) !== null && _a !== void 0 ? _a : []).join('\n') +
        versionCanary + '\n\n' +
        schemaDefs.join('\n\n') +
        `\n\n/* === global aggregate types === */\n` +
        (0, tables_1.crossSchemaTypesForSchemas)(schemaNames) +
        `\n\n/* === lookups === */\n` +
        (0, tables_1.crossSchemaTypesForAllTables)(allTables, config.unprefixedSchema)), customTypeSourceFiles = sourceFilesForCustomTypes(customTypes);
    await pool.end();
    return { ts, customTypeSourceFiles };
};
exports.tsForConfig = tsForConfig;
