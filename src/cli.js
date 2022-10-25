#!/usr/bin/env node

import fs from 'fs/promises'
import path from 'path'
import { createRequire } from 'module'
import { exec as _exec } from 'child_process'
import { promisify } from 'util'

import { program } from 'commander'
import YAML from 'yaml'

import { writeSourcesYML, writeGenerateSchemaName } from './dbt.js'
import { exists, ensureDir, writeFile } from './fs.js'
import {
  declarationsToSourceMap,
  declarationsToDbtSources,
  extractConfigs,
  tablesToDbtModels,
  writeModel,
  writeOperation,
  writeTest,
} from './transform.js'
import { chunkify } from './utils.js'

const exec = promisify(_exec)
const require = createRequire(import.meta.url)
const PACKAGE = require('../package.json')

program
  .requiredOption(
    '-t, --template <dbt project template>',
    'a template dbt_project.yml file which may be extended',
  )
  .requiredOption(
    '-p, --profiles <dbt profiles yml>',
    'a profiles.yml file which may be extended',
  )
  .option(
    '-d, --directory <root directory>',
    'dataform project root directory',
    process.cwd(),
  )
  .option(
    '-r, --rename <replacements...>',
    'a set of key/value replacements, e.g. myschema.mytable yourtable to rename the table',
  )
  .version(PACKAGE.version)
  .parse()

const {
  directory: ROOT,
  profiles: PROFILES,
  rename = [],
  template: TEMPLATE,
} = program.opts()

if (!(await exists(TEMPLATE))) {
  console.error(`Template file ${TEMPLATE} does not exist or is not readable.`)
  process.exit(1)
}

if (!(await exists(PROFILES))) {
  console.error(`Profiles file ${PROFILES} does not exist or is not readable.`)
  process.exit(1)
}

if (!(await exists(ROOT))) {
  console.error(`Root directory ${ROOT} does not exist or is not readable.`)
  process.exit(1)
}

if (!(await exists(path.resolve(ROOT, 'dataform.json')))) {
  console.error(`Root directory ${ROOT} is missing "dataform.json"`)
  process.exit(1)
}

if (rename.length % 2 !== 0) {
  console.error('Invalid replacements given, must be pairs')
  process.exit(1)
}

const RENAMED = chunkify(rename, 2).reduce((acc, [from, to]) => {
  const [schema, table] = from.split('.')
  if (!acc[schema]) acc[schema] = {}
  acc[schema][table] = to
  return acc
}, {})

const DBT_TESTS_DIR = path.resolve(ROOT, 'tests')
const DBT_MODELS_DIR = path.resolve(ROOT, 'models')
const DBT_MACROS_DIR = path.resolve(ROOT, 'macros')

// Utility to apply renaming for tables
const adjustName = (schema, table) => RENAMED[schema]?.[table] || table

// Clean out models and macros first
await Promise.all(
  [DBT_MODELS_DIR, DBT_MACROS_DIR, DBT_TESTS_DIR].map(async (dir) => {
    if (await exists(dir)) await fs.rm(dir, { recursive: true })
    return ensureDir(dir)
  }),
)

// Compile dataform schema if necessary
const DF_JSON_FILE = path.resolve(ROOT, 'df_comp.json')
if (!(await exists(DF_JSON_FILE))) {
  console.debug('compiling dataform')
  await exec(`npx dataform compile --json > ${DF_JSON_FILE}`, { cwd: ROOT })
}
// eslint-disable-next-line import/no-dynamic-require
const DATAFORM_COMPILATION_JSON = require(DF_JSON_FILE)

const profiles = YAML.parse(await fs.readFile(PROFILES, 'utf8'))
// Note: no way to know which is the right prod schema except by convention I guess
const DEFAULT_SCHEMA = Object.values(profiles)[0].outputs.prod.schema

const sources = declarationsToSourceMap(DATAFORM_COMPILATION_JSON)
const configs = await extractConfigs(
  ROOT,
  sources,
  adjustName,
  DATAFORM_COMPILATION_JSON,
)

await Promise.all([
  // Extract sources and write to a sources file
  writeSourcesYML(ROOT, declarationsToDbtSources(DATAFORM_COMPILATION_JSON)),
  // Extract all models docs and tests and write to appropriate files
  ...(
    await tablesToDbtModels(configs, adjustName)
  ).map(async ({ schema, models }) => {
    const dir = path.resolve(DBT_MODELS_DIR, schema)
    return writeFile(dir, `_${schema}__models.yml`, models)
  }),
])

const flags = {
  multiSchema: false,
}
let onRunStart = []
const udfReplacements = {}

await Promise.all(
  configs
    .filter((c) => c.raw.type === 'operation')
    .map(writeOperation(ROOT, udfReplacements, onRunStart)),
)

await Promise.all([
  ...configs
    .filter((c) => c.raw.type === 'assertion')
    .map(writeTest(ROOT, udfReplacements)),
  ...configs
    .filter((c) => !['operation', 'assertion'].includes(c.raw.type))
    .map(writeModel(ROOT, udfReplacements, adjustName, flags, DEFAULT_SCHEMA)),
])

if (flags.multiSchema) {
  console.log(
    `Multiple schemas detected, writing custom schema resolver; see https://docs.getdbt.com/docs/build/custom-schemas#an-alternative-pattern-for-generating-schema-names for information`,
  )
  await writeGenerateSchemaName(ROOT)
}

const project = YAML.parse(await fs.readFile(TEMPLATE, 'utf8'))
onRunStart = [...(project['on-run-start'] || []), ...onRunStart]
if (onRunStart.length) project['on-run-start'] = onRunStart
await writeFile(ROOT, 'dbt_project.yml', YAML.stringify(project))
