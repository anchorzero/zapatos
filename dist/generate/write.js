"use strict";
/*
Zapatos: https://jawj.github.io/zapatos/
Copyright (C) 2020 - 2022 George MacKerron
Released under the MIT licence: see LICENCE file
*/
Object.defineProperty(exports, "__esModule", { value: true });
exports.generateFromConfigFile = exports.generate = void 0;
const fs = require("fs");
const path = require("path");
const config_1 = require("./config");
const legacy = require("./legacy");
const tsOutput_1 = require("./tsOutput");
const header_1 = require("./header");
/**
 * Generate a schema and supporting files and folders given a configuration.
 * @param suppliedConfig An object approximately matching `zapatosconfig.json`.
 */
const generate = async (suppliedConfig) => {
    const config = (0, config_1.finaliseConfig)(suppliedConfig), log = config.progressListener === true ? console.log :
        config.progressListener || (() => void 0), warn = config.warningListener === true ? console.log :
        config.warningListener || (() => void 0), debug = config.debugListener === true ? console.log :
        config.debugListener || (() => void 0), { ts, customTypeSourceFiles } = await (0, tsOutput_1.tsForConfig)(config, debug), folderName = 'zapatos', schemaName = 'schema' + config.outExt, customFolderName = 'custom', eslintrcName = '.eslintrc.json', eslintrcContent = '{\n  "ignorePatterns": [\n    "*"\n  ]\n}', customTypesIndexName = 'index' + config.outExt, customTypesIndexContent = (0, header_1.header)() + `
// this empty declaration appears to fix relative imports in other custom type files
declare module 'zapatos/custom' { }
`, folderTargetPath = path.join(config.outDir, folderName), schemaTargetPath = path.join(folderTargetPath, schemaName), customFolderTargetPath = path.join(folderTargetPath, customFolderName), eslintrcTargetPath = path.join(folderTargetPath, eslintrcName), customTypesIndexTargetPath = path.join(customFolderTargetPath, customTypesIndexName);
    log(`(Re)creating schema folder: ${schemaTargetPath}`);
    fs.mkdirSync(folderTargetPath, { recursive: true });
    log(`Writing generated schema: ${schemaTargetPath}`);
    fs.writeFileSync(schemaTargetPath, ts, { flag: 'w' });
    log(`Writing local ESLint config: ${eslintrcTargetPath}`);
    fs.writeFileSync(eslintrcTargetPath, eslintrcContent, { flag: 'w' });
    if (Object.keys(customTypeSourceFiles).length > 0) {
        fs.mkdirSync(customFolderTargetPath, { recursive: true });
        for (const customTypeFileName in customTypeSourceFiles) {
            const customTypeFilePath = path.join(customFolderTargetPath, customTypeFileName + config.outExt);
            if (fs.existsSync(customTypeFilePath)) {
                log(`Custom type or domain declaration file already exists: ${customTypeFilePath}`);
            }
            else {
                warn(`Writing new custom type or domain placeholder file: ${customTypeFilePath}`);
                const customTypeFileContent = customTypeSourceFiles[customTypeFileName];
                fs.writeFileSync(customTypeFilePath, customTypeFileContent, { flag: 'w' });
            }
        }
        log(`Writing custom types file: ${customTypesIndexTargetPath}`);
        fs.writeFileSync(customTypesIndexTargetPath, customTypesIndexContent, { flag: 'w' });
    }
    legacy.srcWarning(config);
};
exports.generate = generate;
const recursivelyInterpolateEnvVars = (obj) => 
// string? => do the interpolation
typeof obj === "string"
    ? obj.replace(/\{\{\s*([^}\s]+)\s*\}\}/g, (_0, name) => {
        const e = process.env[name];
        if (e === undefined)
            throw new Error(`Environment variable '${name}' is not set`);
        return e;
    })
    : // array? => recurse over its items
        Array.isArray(obj)
            ? obj.map((item) => recursivelyInterpolateEnvVars(item))
            : // object? => recurse over its values (but don't touch the keys)
                obj !== null && typeof obj === "object"
                    ? Object.keys(obj).reduce((memo, key) => {
                        memo[key] = recursivelyInterpolateEnvVars(obj[key]);
                        return memo;
                    }, {})
                    : // anything else (e.g. number)? => pass right through
                        obj;
function generateFromConfigFile() {
    var _a;
    const configFile = "zapatosconfig.json", configJSON = fs.existsSync(configFile)
        ? fs.readFileSync(configFile, { encoding: "utf8" })
        : "{}", argsJSON = (_a = process.argv[2]) !== null && _a !== void 0 ? _a : "{}";
    let fileConfig;
    try {
        fileConfig = recursivelyInterpolateEnvVars(JSON.parse(configJSON));
    }
    catch (err) {
        throw new Error(`If present, zapatosconfig.json must be a valid JSON file, and all referenced environment variables must exist: ${err.message}`);
    }
    let argsConfig;
    try {
        argsConfig = recursivelyInterpolateEnvVars(JSON.parse(argsJSON));
    }
    catch (err) {
        throw new Error(`If present, the argument to Zapatos must be valid JSON, and all referenced environment variables must exist: ${err.message}`);
    }
    return (0, exports.generate)({ ...fileConfig, ...argsConfig });
}
exports.generateFromConfigFile = generateFromConfigFile;
;
