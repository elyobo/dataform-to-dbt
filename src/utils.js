/**
 * Convert an array into even sized chunks
 */
export const chunkify = (arr, size) => {
  // Calculate even sized chunks
  const count = Math.ceil(arr.length / size)
  const chunkSize = Math.ceil(arr.length / count)

  return Array.from(new Array(count)).map((_, idx) =>
    arr.slice(idx * chunkSize, (idx + 1) * chunkSize),
  )
}

/**
 * Eval, but somewhat more safe
 *
 * @see https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/eval#never_use_eval!
 */
export const saferEval = (content) =>
  Function(`"use strict"; return (${content})`)() // eslint-disable-line no-new-func

/**
 * An async version of a pipe function.
 */
export const asyncPipe =
  (...fns) =>
  (initial) =>
    fns.reduce(async (acc, fn) => fn(await acc), Promise.resolve(initial))
