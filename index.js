#!/usr/bin/env node
/**
 * Automatically migrate a Dataform project to DBT
 *
 * requirements
 *  - cp dbt_project.yml dbt_project.yml.template
 *  - profiles.yml must be set up, have a prod output, with a fixed schema
 */
import fs from 'fs/promises'
import path from 'path'
import { createRequire } from 'node:module'
import { exec as _exec } from 'child_process'
import { promisify } from 'util'

import dataform from '@dataform/core'
import { program } from 'commander'
import YAML from 'yaml'

const require = createRequire(import.meta.url)
const PACKAGE = require('./package.json')
const exec = promisify(_exec)

program
  .requiredOption(
    '-t, --template <dbt project template>',
    'a template dbt_project.yml file which may be extended',
  )
  .requiredOption(
    '-p, --profiles <dbt profiles yml>',
    'a profiles.yml file which may be extended',
  )
  .option('-d, --directory <root directory>', 'dataform project root directory', process.cwd())
  .option(
    '-r, --rename <replacements...>',
    'a set of key/value replacements, e.g. myschema.mytable yourtable to rename the table'
  )
  .version(PACKAGE.version)
  .parse()

// Utility to check if a given path exists
const exists = path => fs.access(path).then(() => true).catch(e => false)

const {
  directory: ROOT,
  profiles: PROFILES,
  rename = [],
  template: TEMPLATE,
} = program.opts()

if (!await exists(TEMPLATE)) {
  console.error(`Template file ${TEMPLATE} does not exist or is not readable.`)
  process.exit(1)
}

if (!await exists(PROFILES)) {
  console.error(`Profiles file ${PROFILES} does not exist or is not readable.`)
  process.exit(1)
}

if (!await exists(ROOT)) {
  console.error(`Root directory ${ROOT} does not exist or is not readable.`)
  process.exit(1)
}

if (!await exists(path.resolve(ROOT, 'dataform.json'))) {
  console.error(`Root directory ${ROOT} is missing "dataform.json"`)
  process.exit(1)
}

if (rename.length % 2 !== 0) {
  console.error('Invalid replacements given, must be pairs')
  process.exit(1)
}

const chunkify = (arr, size) => {
  // Calculate even sized chunks
  const count = Math.ceil(arr.length / size)
  const chunkSize = Math.ceil(arr.length / count)

  return Array.from(new Array(count))
    .map((_, idx) => arr.slice(idx * chunkSize, (idx + 1) * chunkSize))
}

const RENAMED = chunkify(rename, 2).reduce((acc, [from, to]) => {
  const [schema, table] = from.split('.')
  if (!acc[schema]) acc[schema] = {}
  acc[schema][table] = to
  return acc
}, {})

process.chdir(ROOT)

const DF_JSON_FILE = path.resolve(ROOT, 'df_comp.json')
const DF_DEFS_DIR = path.resolve(ROOT, 'definitions')
const DF_INCLUDES_DIR = path.resolve(ROOT, 'includes')
const DBT_MODELS_DIR = path.resolve(ROOT, 'models')
const DBT_MACROS_DIR = path.resolve(ROOT, 'macros')
const DBT_SCHEMA_FILE = path.resolve(DBT_MODELS_DIR, '_schema.yml')
const DBT_SOURCES_FILE = path.resolve(DBT_MODELS_DIR, '_sources.yml')

// Use real schemas in prod but the custom one in others
// See https://docs.getdbt.com/docs/build/custom-schemas#an-alternative-pattern-for-generating-schema-names
const GENERATE_SCHEMA_NAME = `
{# Use real schemas in prod, rewrite to single schema in others #}
{# See https://docs.getdbt.com/docs/build/custom-schemas#an-alternative-pattern-for-generating-schema-names #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ generate_schema_name_for_env(custom_schema_name, node) }}
{%- endmacro %}
`

const asyncPipe = (...fns) => initial => fns.reduce(
  async (acc, fn) => fn(await acc),
  Promise.resolve(initial)
)

// Utility to apply renaming for tables
const adjustName = (schema, table) => (RENAMED[schema]?.[table] || table)

// Utility to ensure a directory exists
const ensureDir = async (dir) => {
  if (!await exists(dir)) await fs.mkdir(dir, { recursive: true })
}

// Utility to write a file, ensuring the directory exists first
const writeFile = async (dir, file, content) => {
  await ensureDir(dir)
  await fs.writeFile(path.resolve(dir, file), content)
}

// Convert DF definitions to DBT sources YAML
const declarationsToDbtSources = ({ declarations }) => YAML.stringify(({
  version: 2,
  sources: declarations.reduce((acc, d) => {
    const { schema, name } = d.target
    const table = { name }

    const source = acc.find(s => s.name === schema)
    if (source) source.tables.push(table)
    else acc.push({ name: schema, tables: [table] })

    return acc
  }, []),
}))

// Convert DF definitions to a map of sources to differentiate between sources and refs
const declarationsToSourceMap = ({ declarations }) => declarations.reduce((acc, d) => {
  const { schema, name } = d.target
  acc[schema] = acc[schema] || {}
  acc[schema][name] = true

  // refs without a schema _must_ be unique or DF complains, and so we can figure out
  // that they're a source even without a schema here
  if (acc[name]) throw new Error(`Name clash in sources on ${name}`)
  // The original schema is used to unpack when replacing refs
  acc[name] = schema

  return acc
}, {})

// Convert DF tables to a map of table -> schema
const tablesToSchemaMap = ({ tables }) => tables.reduce((acc, t) => {
  const { schema, name: _name } = t.target
  const name = adjustName(schema, _name)
  if (acc[name]) {
    throw new Error(`Colision in schema map ${name} is ambiguous in ${schema} and ${acc[name]}; consider using --rename to rename ambiguous tables.`)
  }
  acc[name] = schema

  return acc
}, {})

// Create a map of DF operations for identification
const operationsMap = ({ operations = [] }) => operations.reduce((acc, o) => {
  acc[o.target.name] = o
  return acc
}, {})

// Convert DF tables to DBT model definitions
const tablesToDbtModels = async (configs) => {
  const schemas = configs
    .filter(config => config.raw.type !== 'operation')
    .reduce((acc, config) => {
      const schema = config.raw.target.schema
      if (!acc[schema]) acc[schema] = []
      acc[schema].push(config)
      return acc
    }, {})


  return Object.entries(schemas).map(([schema, tables]) => ({
    schema,
    models: YAML.stringify(({
      version: 2,
      models: tables.map((config) => {
        const {
          config: {
            assertions: {
              nonNull = [],
              rowConditions = [],
              uniqueKey = [],
            } = {},
          },
          raw: {
            actionDescriptor: {
              columns: _columns = [],
              description = '',
            } = {},
            target,
          },
        } = config
        const columns = _columns.map(c => ({
          name: c.path.join('.'),
          description: c.description,
        }))

        // Add not null tests
        nonNull.forEach((field) => {
          const existing = columns.find(col => col.name === field)
          if (existing) {
            existing.tests = existing.tests || []
            existing.tests.push('not_null')
          } else {
            columns.push({
              name: field,
              tests: ['not_null'],
            })
          }
        })

        // Add single column unique tests to the column itself
        const singleUnique = (
          uniqueKey && (
            (typeof uniqueKey === 'string' && uniqueKey)
            || (Array.isArray(uniqueKey) && uniqueKey.length === 1 && uniqueKey[0])
          )
        )
        if (singleUnique) {
          const existing = columns.find(col => col.name === singleUnique)
          if (existing) {
            existing.tests = existing.tests || []
            existing.tests.push('unique')
          } else {
            columns.push({
              name: singleUnique,
              tests: ['unique'],
            })
          }
        }

        const tableTests = []

        // Add multi column unique tests to the table
        const multiUnique = Array.isArray(uniqueKey) && uniqueKey.length > 1 && uniqueKey
        if (multiUnique) {
          tableTests.push({
            'dbt_utils.unique_combination_of_columns': {
              combination_of_columns: multiUnique,
            },
          })
        }

        // Add generic row condition tests to the table
        tableTests.push(...rowConditions.map(expression => ({
          'dbt_utils.expression_is_true': { expression },
        })))

        return {
          name: adjustName(schema, target.name),
          description,
          tests: tableTests.length ? tableTests : undefined,
          columns,
        }
      }),
    }))
  }))
}

// Recursively retrieve files
const getFiles = async (dir, recurse = true) => {
  const dirents = await fs.readdir(dir, { withFileTypes: true })
  const results = await Promise.all(dirents.map((dirent) => {
    const absolute = path.resolve(dir, dirent.name)

    if (dirent.isDirectory()) return (recurse ?  getFiles(absolute) : undefined)

    return [{
      file: {
        absolute,
        type: dirent.name.split('.').pop(),
        base: path.basename(dirent.name, `.${dirent.name.split('.').pop()}`)
      },
      dir: {
        name: path.basename(dir),
      },
    }]
  }).filter(Boolean))

  return results.flat()
}

// Resolve DF includes for use in parsing
const resolveIncludes = async () => {
  const files = await getFiles(DF_INCLUDES_DIR, false)
  return await files.reduce(async (getAcc, file) => {
    const acc = await getAcc
    acc[file.file.base] = await import(file.file.absolute)
    return acc
  }, Promise.resolve({}))
}

const saferEval = (content, ...args) => Function(`"use strict"; return (${content})`)()
const parseExtractor = includes => (content) => {
  // Collect payloads pushed to dataform.sqlxAction
  const collected = []
  const dataform = {
    sqlxAction: payload => collected.push(payload)
  }

  // Execute eval-ed code, passing in required localised globals
  saferEval(`
    function unpack(includes, dataform) {
      const { ${Object.keys(includes).join(', ')} } = includes;
      ${content}
    }
  `)(includes, dataform)

  // Sanity check - should only be called once
  if (collected.length !== 1) {
    throw new Error(`dataform parse extraction failed, ${collected.length} calls`)
  }

  return collected[0]
}

// Replace a single reference, given a set of sources first
const replaceReference = sources => (a, b) => {
  const table = b || a
  const schema = b ? a : sources[table]
  const ref = sources[schema]?.[table]
    ? `source('${schema}', '${table}')`
    : `ref('${adjustName(schema, table)}')`

  return `{{ ${ref} }}`
}

// Identify and replace references with either a source or local ref call
const REF_PATTERN = /\$\{\s*ref\(['"]([^'"]+)['"](?:,\s*['"]([^'"]+)['"])?\)\s*\}/g
const replaceReferences = (sources) => {
  const replace = replaceReference(sources)
  return src => src.replace(REF_PATTERN, (_, a, b) => replace(a, b))
}

const cleanSqlBlock = (block) => {
  const trimmed = replaceMacroPlaceholders(block?.trim() || '')

  return [';', '}'].includes(trimmed[trimmed.length - 1]) ? trimmed : `${trimmed};`
}

/**
 * Extracts config from @dataform/core's compiler, which produces reasonably safe
 * looking code for evaluation.
 *
 * It's necessary to inject the global includes and to mock a dataform function
 * to collect the config, but parses reliably and provides sqlx with blocks like
 * config and pre-operations removed more accurately than regex extraction does.
 */
const extractConfigs = async ({ operations, tables }, sources) => {
  const includes = await resolveIncludes()
  const extractor = parseExtractor(includes)
  const base = [
    ...tables,
    ...operations.map(op => ({ ...op, type: 'operation' })),
  ]
  const parsed = await Promise.all(base.map(table => {
    const absolute = path.resolve(ROOT, table.fileName)
    return fs
      .readFile(absolute, 'utf8')
      .then(replaceIncludes(includes))
      .then(content => ({
        content,
        file: {
          absolute,
          base: path.basename(absolute, `.${absolute.split('.').pop()}`),
        },
        dir: {
          name: path.basename(path.dirname(absolute)),
        },
        raw: table,
        fileName: table.fileName,
        compiled: extractor(dataform.compiler(content, table.fileName))
      }))
  }))
  const context = { ref: replaceReference(sources) }
  const getSql = (table) => {
    const sqls = table.compiled.sqlContextable(context)
    if (!sqls.length) throw new Error(`No SQL in ${table.fileName}, expected one or more.`)
    // Normal models shouldn't have a trailing semicolon, but operations should
    const sql = sqls.map(cleanSqlBlock).join('\n\n').replace(/;$/, table.raw.type === 'operation' ? ';' : '')

    const preops = table.compiled.preOperationsContextable?.(context) || []
    if (!preops.length) return sql

    const preop = preops.map(cleanSqlBlock).join('\n\n')

    return `{% call set_sql_header(config) %}\n${preop}\n{%- endcall %}\n\n${sql}`
  }

  return parsed.map(table => ({
    ...table,
    config: table.compiled.sqlxConfig,
    sql: getSql(table),
  }))
}

// Replace dataform includes with DBT macros
const INCLUDE_RE = /\$\{(?!\s*ref\()([^}]+)\}/g
const replaceIncludes = includes => async (content) => {
  const map = await Promise.all(
    Array.from(content.matchAll(INCLUDE_RE))
      .map(async ([, include]) => {
        const parts = include.split('.')
        const macro = parts.join('__')
          .replace(/([a-z])([A-Z])/g, (_, a, b) => `${a}_${b}`)
          .replace(/[^A-Z_]/gi, '')
          .toLowerCase()

        let src
        if (include.includes('(')) {
          src = `-- Unhandled ${include}`
          console.warn(`Unable to handle function invocations in includes, replace macro ${macro}`)
        } else {
          const file = parts.shift()
          src = parts.reduce((acc, key) => acc[key], includes[file])
          src = `${src}`.trim()
        }

        await writeFile(
          DBT_MACROS_DIR,
          `${macro}.sql`,
          `{% macro ${macro}() %}\n${src}\n{% endmacro %}`,
        )

        return { include, macro }
      })
  )
  .then(res => res.reduce((acc, inc) => {
    const { include, macro } = inc
    // NOTE(@elyobo) this happens before dataform parsing and curly braces break that, so we do a
    // two step replacement after parsing
    acc[include] = `--MACRO ${macro}() MACRO--`
    return acc
  }, {}))

  return content.replace(INCLUDE_RE, (_, include) => map[include])
}

// Replace macro placeholders from above
const replaceMacroPlaceholders = content => content.replace(
  /--MACRO (.*) MACRO--/g,
  (_, macro) => `{{ ${macro} }}`,
)

// Replace temporary tables with DBT models
const TEMP_RE = /create(?:\s+or\s+replace)?\s+(?:temp(?:orary)?\s+)?table\s+([a-zA-Z0-9_]+)\s+as\s+([^;]+);/img
const replaceTempTables = (schema, model) => async (content) => {
  const temps = await Promise.all(
    Array.from(content.matchAll(TEMP_RE))
      .map(async ([, name, sql]) => {
        const tmpModel = `_${name.toLowerCase().replace(/^_+/, '')}`
        console.warn(`Detected temporary table ${name} in ${schema}.${model}, writing to ${schema}.${tmpModel}`)
        await writeFile(
          path.resolve(DBT_MODELS_DIR, schema),
          `${tmpModel}.sql`,
          `{{ config(materialized='table') }}\n\n${sql}`
        )

        return { name, ref: tmpModel }
      })
  )
  .then(res => res.reduce((acc, temp) => {
    const { name, ref } = temp
    acc[name] = `{{ ref('${ref}') }} AS ${name}`
    return acc
  }, {}))

  const tables = Object.keys(temps).join('|')
  // If no temp tables, nothing to replace
  if (!tables.length) return content

  const usage = new RegExp(`(?<=(?:FROM|JOIN)\\s+)(${tables})\\b`, 'mi')

  return content
    // Drop original definitions entirely
    .replace(TEMP_RE, (_, name) => '')
    // And replace subsequent calls to them
    .replace(usage, (_, table) => temps[table])
}

// Replace the schema on defined permanent UDF creations with a dynamically
// substituted version
const UDF_RE = /(?<=(?:create(?:\s+or\s+replace)?\s+?function\s+))([a-zA-Z0-9_\.]+)/ig
const replaceUdfSchema = store => content => (
  content.replace(UDF_RE, (_, fn) => {
    const [, name] = fn.split('.')
    const replacement = `{{ target.schema }}.${name}`
    store[fn] = replacement
    return replacement
  })
)

// Replace permanent UDF usage for pipeline local functions (detected above)
// with a dynamically substituted one
const replaceUdfSchemaUsage = (replacements) => {
  const udfs = Object.keys(replacements).join('|').replace(/\./g, '\\.')
  if (!udfs) return content => content
  const usage = new RegExp(`\\b(${udfs})\\b`, 'g')
  return content => content.replace(usage, (_, udf) => replacements[udf])
}


// Clean out models and macros first
await Promise.all([DBT_MODELS_DIR, DBT_MACROS_DIR].map(async (dir) => {
  if (await exists(dir)) await fs.rm(dir, { recursive: true })
  return ensureDir(dir)
}))

// Compile dataform schema if necessary
if (!await exists(DF_JSON_FILE)) {
  console.debug('compiling dataform')
  await exec(`npx dataform compile --json > ${DF_JSON_FILE}`)
}

const profiles = YAML.parse(await fs.readFile(PROFILES, 'utf8'))
// Note: no way to know which is the right prod schema except by convention I guess
const defaultSchema = Object.values(profiles)[0].outputs.prod.schema

// Read dataform metadata
const df = require(DF_JSON_FILE)
const sources = declarationsToSourceMap(df)
const configs = await extractConfigs(df, sources)

// Extract sources and write to a sources file
// Extract all models and write to appropriate model files
await Promise.all([
  fs.writeFile(DBT_SOURCES_FILE, declarationsToDbtSources(df)),
  ...(await tablesToDbtModels(configs)).map(async ({ schema, models }) => {
    const dir = path.resolve(DBT_MODELS_DIR, schema)
    await ensureDir(dir)
    return fs.writeFile(path.resolve(dir, `_${schema}__models.yml`), models)
  }),
])

// TODO handle standalone assertions too?
const unref = replaceReferences(sources)
const tables = tablesToSchemaMap(df)
const operations = operationsMap(df)

let onRunStart = []

const udfReplacements = {}
for (const config of configs.filter(c => c.raw.type === 'operation')) {
  const {
    file: { base: name },
    dir: { name: schema },
    sql,
  } = config
  const src = await asyncPipe(
    replaceTempTables(schema, name),
    replaceUdfSchema(udfReplacements),
    x => x.trim(),
  )(sql)

  const dest = path.resolve(DBT_MACROS_DIR, `${name}.sql`)
  const macroName = `operation_${name}`
  onRunStart.push(`{{ ${macroName}() }}`)
  await fs.writeFile(dest, `{% macro ${macroName}() %}\n${src}\n{% endmacro %}`)
}

let multiSchema = false
for (const config of configs.filter(c => c.raw.type !== 'operation')) {
  const {
    file: { base },
    dir: { name: schema },
    sql,
  } = config
  const name = adjustName(schema, base)
  const src = await asyncPipe(
    replaceTempTables(schema, base),
    replaceUdfSchemaUsage(udfReplacements),
    str => str.trim(),
  )(sql)

  if (!schema) {
    console.error(`Unknown schema for ${name}`)
    console.log(name)
    console.log(f)
    process.exit()
  }

  const destDir = path.resolve(DBT_MODELS_DIR, schema)
  await ensureDir(destDir)

  const dest = path.resolve(destDir, `${name}.sql`)
  const configHeader = defaultSchema === schema ? '' : `{{ config(schema='${schema}') }}\n\n`
  multiSchema = multiSchema || Boolean(configHeader)
  await fs.writeFile(dest, `${configHeader}${src}`)
}

if (multiSchema) {
  console.log(`Multiple schemas detected, writing custom schema resolver; see https://docs.getdbt.com/docs/build/custom-schemas#an-alternative-pattern-for-generating-schema-names for information`)
  await fs.writeFile(path.resolve(DBT_MACROS_DIR, 'get_custom_schema.sql'), GENERATE_SCHEMA_NAME)
}

const project = YAML.parse(await fs.readFile(TEMPLATE, 'utf8'))
onRunStart = [...project['on-run-start'] || [], ...onRunStart]
if (onRunStart.length) project['on-run-start'] = onRunStart
await fs.writeFile(path.resolve(ROOT, 'dbt_project.yml'), YAML.stringify(project))
