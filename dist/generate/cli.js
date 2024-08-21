#!/usr/bin/env node
"use strict";
// ^^ this shebang is for the compiled JS file, not the TS source
Object.defineProperty(exports, "__esModule", { value: true });
/*
Zapatos: https://jawj.github.io/zapatos/
Copyright (C) 2020 - 2022 George MacKerron
Released under the MIT licence: see LICENCE file
*/
const _1 = require(".");
void (async () => {
    await (0, _1.generateFromConfigFile)();
})();
