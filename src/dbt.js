import path from 'path'

import { writeFile } from './fs.js'

// Hopefully adequate python output
const quote = (value) => {
  if (value === null) return 'None'
  if (Array.isArray(value)) return `[${value.map(quote).join(', ')}]`
  switch (typeof value) {
    case 'number':
      return value
    case 'string':
      return `'${value.replace(/'/g, "\\'")}'`
    case 'boolean':
      return value ? 'True' : 'False'
    case 'object':
      return `{${Object.entries(value)
        .map((entry) => entry.map(quote).join(': '))
        .join(', ')}}`
    default:
      throw new Error(
        `Unsupported data type ${typeof value} writing config header`,
      )
  }
}

// Write a config header
export const buildConfigHeader = (config) => {
  const options = Object.entries(config)
    .filter(([, v]) => v !== undefined)
    .map(([k, v]) => `    ${k} = ${quote(v)},`)
    .join('\n')

  return options ? `{{\n  config(\n${options}\n  )\n}}\n\n` : ''
}

// Use real schemas in prod but the custom one in others
// See https://docs.getdbt.com/docs/build/custom-schemas#an-alternative-pattern-for-generating-schema-names
const GENERATE_SCHEMA_NAME = `
{# Use real schemas in prod, rewrite to single schema in others #}
{# See https://docs.getdbt.com/docs/build/custom-schemas#an-alternative-pattern-for-generating-schema-names #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ generate_schema_name_for_env(custom_schema_name, node) }}
{%- endmacro %}

{# Prefix non-production tables with their schema #}
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {# Default handling to calculate table name #}
    {%- if custom_alias_name is none -%}
        {%- set node_name = node.name -%}
    {%- else -%}
        {%- set node_name = custom_alias_name | trim -%}
    {%- endif -%}

    {%- if target.name == 'prod' -%}
        {# No prefix for prod #}
        {{ node_name }}
    {%- else -%}
        {#- Get the custom schema name -#}
        {%- set schema = node.unrendered_config.schema | trim %}

        {#- Highlight missing schemas, _ prefix to sort early -#}
        {%- if not schema -%}
            {%- set schema = '_NO_SCHEMA' %}
        {%- endif -%}

        {{ schema ~ "__" ~ node_name }}
    {%- endif -%}
{%- endmacro %}
`.trim()

export const writeGenerateSchemaName = async (root) =>
  writeFile(
    path.resolve(root, 'macros'),
    'get_custom_schema.sql',
    GENERATE_SCHEMA_NAME,
  )

export const writeSourcesYML = async (root, content) =>
  writeFile(path.resolve(root, 'models'), '_sources.yml', content)
