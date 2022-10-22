import { parsePartitionBy } from './dataform.js'

describe('parsePartitionBy()', () => {
  it.each([
    [
      'DATE(some_field)',
      { field: 'some_field', data_type: 'date', granularity: 'day' },
    ],
  ])('"%p" is parsed as %p', (input, output) => {
    expect.hasAssertions()
    expect(parsePartitionBy(input)).toStrictEqual(output)
  })
})
