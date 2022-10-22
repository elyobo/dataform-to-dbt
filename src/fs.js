import fs from 'fs/promises'
import path from 'path'

/**
 * Check whether a given path exists
 */
export const exists = (location) =>
  fs
    .access(location)
    .then(() => true)
    .catch(() => false)

/**
 * Utility to ensure a directory exists
 */
export const ensureDir = async (dir) => {
  if (!(await exists(dir))) await fs.mkdir(dir, { recursive: true })
}

/**
 * Utility to write a file, ensuring the directory exists first
 */
export const writeFile = async (dir, file, content) => {
  await ensureDir(dir)
  await fs.writeFile(path.resolve(dir, file), content)
}

/**
 * Recursively retrieve files
 */
export const getFiles = async (dir, recurse = true) => {
  const dirents = await fs.readdir(dir, { withFileTypes: true })
  const results = await Promise.all(
    dirents
      .map((dirent) => {
        const absolute = path.resolve(dir, dirent.name)

        if (dirent.isDirectory())
          return recurse ? getFiles(absolute) : undefined

        return [
          {
            file: {
              absolute,
              type: dirent.name.split('.').pop(),
              base: path.basename(
                dirent.name,
                `.${dirent.name.split('.').pop()}`,
              ),
            },
            dir: {
              name: path.basename(dir),
            },
          },
        ]
      })
      .filter(Boolean),
  )

  return results.flat()
}
