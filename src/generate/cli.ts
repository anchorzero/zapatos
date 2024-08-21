#!/usr/bin/env node
// ^^ this shebang is for the compiled JS file, not the TS source

/*
Zapatos: https://jawj.github.io/zapatos/
Copyright (C) 2020 - 2022 George MacKerron
Released under the MIT licence: see LICENCE file
*/

import { generateFromConfigFile } from ".";

void (async () => {
  await generateFromConfigFile();
})();
