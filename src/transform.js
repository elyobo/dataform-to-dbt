import fs from 'fs/promises'
import path from 'path'
import YAML from 'yaml'

import {
  parseExtractor,
  parsePartitionBy,
  resolveIncludes,
} from './dataform.js'
import { buildConfigHeader } from './dbt.js'
import { writeFile } from './fs.js'
import { asyncPipe } from './utils.js'

/**
 * Convert dataform definitions to DBT sources YAML
 */
export const declarationsToDbtSources = ({ declarations }) =>
  YAML.stringify({
    version: 2,
    sources: declarations.reduce((acc, d) => {
      const { schema, name } = d.target
      const table = { name }

      const source = acc.find((s) => s.name === schema)
      if (source) source.tables.push(table)
      else acc.push({ name: schema, tables: [table] })

      return acc
    }, []),
  })

/**
 * Convert dataform definitions to a map of sources to differentiate between sources and refs
 */
export const declarationsToSourceMap = ({ declarations }) =>
  declarations.reduce((acc, d) => {
    const { schema, name } = d.target
    acc[schema] = acc[schema] || {}
    acc[schema][name] = true

    // refs without a schema _must_ be unique or dataform complains, and so we can figure out
    // that they're a source even without a schema here
    if (acc[name]) throw new Error(`Name clash in sources on ${name}`)
    // The original schema is used to unpack when replacing refs
    acc[name] = schema

    return acc
  }, {})

/**
 * Convert dataform tables to DBT model definitions
 */
export const tablesToDbtModels = async (configs, adjustName) => {
  const directories = configs
    .filter((config) => !['operation', 'assertion'].includes(config.raw.type))
    .reduce((acc, config) => {
      const dirPath = path.dirname(config.fileName)
      //  Handle models in the root definitions directoory
      const dirname = dirPath.includes('/') ? path.basename(dirPath) : ''

      if (!acc[dirname]) acc[dirname] = []
      acc[dirname].push(config)
      return acc
    }, {})

  return Object.entries(directories).map(([directory, tables]) => ({
    directory,
    models: YAML.stringify({
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
            actionDescriptor: { columns: _columns = [], description = '' } = {},
            target,
          },
        } = config
        const columns = _columns.map((c) => ({
          name: c.path.join('.'),
          description: c.description,
        }))

        // Add not null tests
        nonNull.forEach((field) => {
          const existing = columns.find((col) => col.name === field)
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
        const singleUnique =
          uniqueKey &&
          ((typeof uniqueKey === 'string' && uniqueKey) ||
            (Array.isArray(uniqueKey) &&
              uniqueKey.length === 1 &&
              uniqueKey[0]))
        if (singleUnique) {
          const existing = columns.find((col) => col.name === singleUnique)
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
        const multiUnique =
          Array.isArray(uniqueKey) && uniqueKey.length > 1 && uniqueKey
        if (multiUnique) {
          tableTests.push({
            'dbt_utils.unique_combination_of_columns': {
              combination_of_columns: multiUnique,
            },
          })
        }

        // Add generic row condition tests to the table
        tableTests.push(
          ...rowConditions.map((expression) => ({
            'dbt_utils.expression_is_true': { expression },
          })),
        )

        return {
          name: adjustName(target.schema, target.name),
          description,
          tests: tableTests.length ? tableTests : undefined,
          columns,
        }
      }),
    }),
  }))
}

// Replace macro placeholders from above
const replaceMacroPlaceholders = (content) =>
  content.replace(/--MACRO (.*) MACRO--/g, (_, macro) => `{{ ${macro} }}`)

const cleanSqlBlock = (block) => {
  const trimmed = replaceMacroPlaceholders(block?.trim() || '')

  return [';', '}'].includes(trimmed[trimmed.length - 1])
    ? trimmed
    : `${trimmed};`
}

// Replace a single reference, given a set of sources first
const replaceReference = (sources, adjustName) => (a, b) => {
  const table = b || a
  const schema = b ? a : sources[table]
  const ref = sources[schema]?.[table]
    ? `source('${schema}', '${table}')`
    : `ref('${adjustName(schema, table)}')`

  return `{{ ${ref} }}`
}

// Replace dataform includes with DBT macros
const INCLUDE_RE = /\$\{(?!\s*ref\()([^}]+)\}/g
const replaceIncludes = (root, includes) => async (content) => {
  const macrosDir = path.resolve(root, 'macros')

  const map = await Promise.all(
    Array.from(content.matchAll(INCLUDE_RE)).map(async ([, include]) => {
      const parts = include.split('.')
      const macro = parts
        .join('__')
        .replace(/([a-z])([A-Z])/g, (_, a, b) => `${a}_${b}`)
        .replace(/[^A-Z_]/gi, '')
        .toLowerCase()

      let src
      if (include.includes('(')) {
        src = `-- Unhandled ${include}`
        console.warn(
          `Unable to handle function invocations in includes, replace macro ${macro}`,
        )
      } else {
        const file = parts.shift()
        src = parts.reduce((acc, key) => acc[key], includes[file])
        src = `${src}`.trim()
      }

      await writeFile(
        macrosDir,
        `${macro}.sql`,
        `{% macro ${macro}() %}\n${src}\n{% endmacro %}`,
      )

      return { include, macro }
    }),
  ).then((res) =>
    res.reduce((acc, inc) => {
      const { include, macro } = inc
      // NOTE(@elyobo) this happens before dataform parsing and curly braces break that, so we do a
      // two step replacement after parsing
      acc[include] = `--MACRO ${macro}() MACRO--`
      return acc
    }, {}),
  )

  return content.replace(INCLUDE_RE, (_, include) => map[include])
}

/**
 * Extracts config from @dataform/core's compiler, which produces reasonably safe
 * looking code for evaluation.
 *
 * It's necessary to inject the global includes and to mock a dataform function
 * to collect the config, but parses reliably and provides sqlx with blocks like
 * config and pre-operations removed more accurately than regex extraction does.
 */
export const extractConfigs = async (
  root,
  sources,
  adjustName,
  { assertions, operations, tables },
) => {
  const includes = await resolveIncludes(root)
  const extractor = parseExtractor(includes)
  const base = [
    ...tables,
    ...operations.map((op) => ({ ...op, type: 'operation' })),
    ...assertions
      // Only manual assertions
      .filter(
        (assert) => !/(rowConditions|uniqueKey_[0-9]+)$/.test(assert.name),
      )
      .map((assert) => ({ ...assert, type: 'assertion' })),
  ]
  const parsed = await Promise.all(
    base.map((table) => {
      const absolute = path.resolve(root, table.fileName)
      return fs
        .readFile(absolute, 'utf8')
        .then(replaceIncludes(root, includes))
        .then((content) => ({
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
          compiled: extractor(content, table.fileName),
        }))
    }),
  )
  const context = { ref: replaceReference(sources, adjustName) }
  const getSql = (table) => {
    const sqls = table.compiled.sqlContextable(context)
    if (!sqls.length)
      throw new Error(`No SQL in ${table.fileName}, expected one or more.`)
    // Normal models shouldn't have a trailing semicolon, but operations should
    const sql = sqls
      .map(cleanSqlBlock)
      .join('\n\n')
      .replace(/;$/, table.raw.type === 'operation' ? ';' : '')

    const preops = table.compiled.preOperationsContextable?.(context) || []
    if (!preops.length) return sql

    const preop = preops.map(cleanSqlBlock).join('\n\n')

    return `{% call set_sql_header(config) %}\n${preop}\n{%- endcall %}\n\n${sql}`
  }

  return parsed.map((table) => ({
    ...table,
    config: table.compiled.sqlxConfig,
    sql: getSql(table),
  }))
}

// Replace temporary tables with DBT models
const TEMP_RE =
  /create(?:\s+or\s+replace)?\s+(?:temp(?:orary)?\s+)?table\s+([a-zA-Z0-9_]+)\s+as\s+([^;]+);/gim
export const replaceTempTables = (root, schema, model) => async (content) => {
  const temps = await Promise.all(
    Array.from(content.matchAll(TEMP_RE)).map(async ([, name, sql]) => {
      const tmpModel = `_${name.toLowerCase().replace(/^_+/, '')}`
      console.warn(
        `Detected temporary table ${name} in ${schema}.${model}, writing to ${schema}.${tmpModel}`,
      )
      await writeFile(
        path.resolve(root, 'models', schema),
        `${tmpModel}.sql`,
        `{{ config(materialized='table') }}\n\n${sql}`,
      )

      return { name, ref: tmpModel }
    }),
  ).then((res) =>
    res.reduce((acc, temp) => {
      const { name, ref } = temp
      acc[name] = `{{ ref('${ref}') }} AS ${name}`
      return acc
    }, {}),
  )

  const tables = Object.keys(temps).join('|')
  // If no temp tables, nothing to replace
  if (!tables.length) return content

  const usage = new RegExp(`(?<=(?:FROM|JOIN)\\s+)(${tables})\\b`, 'mi')

  return (
    content
      // Drop original definitions entirely
      .replace(TEMP_RE, () => '')
      // And replace subsequent calls to them
      .replace(usage, (_, table) => temps[table])
  )
}

// Replace the schema on defined permanent UDF creations with a dynamically
// substituted version
const UDF_RE =
  /(?<=(?:create(?:\s+or\s+replace)?\s+?function\s+))([a-zA-Z0-9_.]+)/gi
export const replaceUdfSchema = (store) => (content) =>
  content.replace(UDF_RE, (_, fn) => {
    const [, name] = fn.split('.')
    const replacement = `{{ target.schema }}.${name}`
    store[fn] = replacement // eslint-disable-line no-param-reassign
    return replacement
  })

// Replace permanent UDF usage for pipeline local functions (detected above)
// with a dynamically substituted one
export const replaceUdfSchemaUsage = (replacements) => {
  const udfs = Object.keys(replacements).join('|').replace(/\./g, '\\.')
  if (!udfs) return (content) => content
  const usage = new RegExp(`\\b(${udfs})\\b`, 'g')
  return (content) => content.replace(usage, (_, udf) => replacements[udf])
}

/**
 * Write an operation.
 */
export const writeOperation =
  (root, udfReplacements, onRunStart) => async (config) => {
    const {
      file: { base: name },
      dir: { name: schema },
      sql,
    } = config
    const src = await asyncPipe(
      replaceTempTables(root, schema, name),
      replaceUdfSchema(udfReplacements),
      (x) => x.trim(),
    )(sql)

    const macroName = `operation_${name}`
    onRunStart.push(`{{ ${macroName}() }}`)
    await writeFile(
      path.resolve(root, 'macros'),
      `${name}.sql`,
      `{% macro ${macroName}() %}\n${src}\n{% endmacro %}\n`,
    )
  }

/**
 * Write a test.
 */
export const writeTest = (root, udfReplacements) => async (config) => {
  const {
    file: { base: name },
    dir: { name: schema },
    raw: { tags = [] },
    sql,
  } = config
  const src = await asyncPipe(
    replaceTempTables(root, schema, name),
    replaceUdfSchemaUsage(udfReplacements),
    (x) => x.trim(),
  )(sql)
  const configHeader = buildConfigHeader({
    tags: tags.length ? tags : undefined,
  })

  await writeFile(
    path.resolve(root, 'tests'),
    `${configHeader}${name}.sql`,
    `${src}\n`,
  )
}

/**
 * Write a model.
 */
export const writeModel =
  (root, udfReplacements, adjustName, flags, defaultSchema) =>
  async (config) => {
    const {
      config: {
        bigquery: {
          clusterBy,
          partitionBy,
          partitionExpirationDays,
          requirePartitionFilter,
        } = {},
      },
      dir: { name: schema },
      file: { base },
      raw: { tags = [], type },
      sql,
    } = config

    const name = adjustName(schema, base)
    const src = await asyncPipe(
      replaceTempTables(root, schema, base),
      replaceUdfSchemaUsage(udfReplacements),
      (str) => str.trim(),
    )(sql)

    if (!schema) {
      console.error(`Unknown schema for ${name}`)
      process.exit(1)
    }

    const dbtConfig = {
      schema: defaultSchema === schema ? undefined : schema,
      materialized: type === 'table' ? undefined : type,
      tags: tags.length ? tags : undefined,
      partition_by: parsePartitionBy(partitionBy),
      require_partition_filter: partitionBy
        ? requirePartitionFilter
        : undefined,
      partition_expiration_days: partitionBy
        ? partitionExpirationDays
        : undefined,
      cluster_by: clusterBy,
    }
    // eslint-disable-next-line no-param-reassign
    flags.multiSchema = flags.multiSchema || Boolean(dbtConfig.schema)
    const configHeader = buildConfigHeader(dbtConfig)
    await writeFile(
      path.resolve(root, 'models', schema),
      `${name}.sql`,
      `${configHeader}${src}\n`,
    )
  }
