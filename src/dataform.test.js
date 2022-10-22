import { parsePartitionBy } from './dataform.js'

describe('parsePartitionBy()', () => {
  it.each([
    [
      'DATE(some_field)',
      { field: 'some_field', data_type: 'date', granularity: 'day' },
    ],
    [
      'DATETIME_TRUNC(some_field, HOUR)',
      { field: 'some_field', data_type: 'datetime', granularity: 'hour' },
    ],
    [
      'TIMESTAMP_TRUNC(some_field, HOUR)',
      { field: 'some_field', data_type: 'timestamp', granularity: 'hour' },
    ],
    [
      'TIMESTAMP_TRUNC(some_field, MONTH)',
      { field: 'some_field', data_type: 'timestamp', granularity: 'month' },
    ],
    [
      'RANGE_BUCKET(some_field, GENERATE_ARRAY(0, 1000000, 1000))',
      {
        field: 'some_field',
        data_type: 'int64',
        range: { start: 0, end: 1000000, interval: 1000 },
      },
    ],
  ])('%p is parsed as %p', (input, output) => {
    expect.hasAssertions()
    expect(parsePartitionBy(input)).toStrictEqual(output)
  })
})
