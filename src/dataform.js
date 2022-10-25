import path from 'path'
import dataform from '@dataform/core'

import { getFiles } from './fs.js'
import { saferEval } from './utils.js'

/**
 * Parse a dataform partition by clause to get a dbt version
 */
export const parsePartitionBy = (value) => {
  if (!value) return undefined
  // e.g. TIMESTAMP_TRUNC(<date_column>, MONTH)
  const trunc = value.match(
    /^(date|datetime|timestamp)_trunc\(([^,]+),\s*([^)]+)\)$/i,
  )
  if (trunc) {
    const [, dataType, field, granularity] = trunc
    return {
      // dbt passes through date type / day granularity fields unmodified, see #9
      field:
        dataType.toLowerCase() === 'date' && granularity.toLowerCase() === 'day'
          ? value
          : field,
      data_type: dataType.toLowerCase(),
      granularity: granularity.toLowerCase(),
    }
  }

  // e.g. DATE(<date_column>)
  const date = value.match(/^date\(([^)]+)\)$/i)
  if (date) return { field: value, data_type: 'date', granularity: 'day' }

  // e.g. RANGE_BUCKET(<integer_column>, GENERATE_ARRAY(0, 1000000, 1000))
  const int = value.match(
    /^range_bucket\(([^,]+),\s*generate_array\((\d+),\s*(\d+),\s*(\d+)\)\)$/i,
  )
  if (int) {
    const [, field] = int
    const [start, end, interval] = int.slice(2).map(Number)

    return {
      field,
      data_type: 'int64',
      range: {
        start,
        end,
        interval,
      },
    }
  }

  throw new Error(`Unable to parse partitioning clause: ${value}`)
}

/**
 * Resolve dataform includes for use in parsing
 */
export const resolveIncludes = async (root) => {
  const files = await getFiles(path.resolve(root, 'includes'), false)
  return files.reduce(async (getAcc, file) => {
    const acc = await getAcc
    acc[file.file.base] = await import(file.file.absolute)
    return acc
  }, Promise.resolve({}))
}

/**
 * Extract parsed dataform data.
 */
export const parseExtractor = (includes) => (content, fileName) => {
  const parsed = dataform.compiler(content, fileName)

  // Collect payloads pushed to dataform.sqlxAction
  const collected = []
  const collector = {
    sqlxAction: (payload) => collected.push(payload),
  }

  // Execute eval-ed code, passing in required localised globals
  saferEval(`
    function unpack(includes, dataform) {
      const { ${Object.keys(includes).join(', ')} } = includes;
      ${parsed}
    }
  `)(includes, collector)

  // Sanity check - should only be called once
  if (collected.length !== 1) {
    throw new Error(
      `dataform parse extraction failed, ${collected.length} calls`,
    )
  }

  return collected[0]
}
