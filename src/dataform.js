// Parse a dataform partition by clause to get a dbt version
export const parsePartitionBy = (value) => {
  if (!value) return undefined
  const trunc = value.match(
    /^(date|datetime|timestamp)_trunc\(([^,]+),\s*([^)]+)\)$/i,
  )
  if (trunc) {
    const [, dataType, field, granularity] = trunc
    return {
      field,
      data_type: dataType.toLowerCase(),
      granularity: granularity.toLowerCase(),
    }
  }

  const date = value.match(/^date\(([^)]+)\)$/i)
  if (date) return { field: date[1], data_type: 'date', granularity: 'day' }

  // RANGE_BUCKET(<integer_column>, GENERATE_ARRAY(0, 1000000, 1000))"
  const int = value.match(
    /^range_bucket\(([^,]+),\s*generate_array\((\d+),\s*(\d+),\s*(\d+)$/i,
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
