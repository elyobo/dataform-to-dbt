module.exports = {
  extends: [
    'eslint:recommended',
    'airbnb-base',
    'plugin:n/recommended',
    'prettier',
  ],
  env: {
    browser: false,
    es2019: true,
    node: true,
  },
  overrides: [
    {
      files: ['**/*.test.js'],
      extends: [
        'plugin:jest/recommended',
        'plugin:jest/style',
        'plugin:jest/all',
      ],
      plugins: ['jest'],
    },
  ],
  parserOptions: {
    ecmaVersion: 'latest',
    sourceType: 'module',
  },
  rules: {
    'import/extensions': ['error', 'ignorePackages'],
    'import/prefer-default-export': 'off',
    'n/no-process-exit': 'off',
    // Fails to handle an include based ignore, smdh
    'n/no-unpublished-import': 'off',
    'no-console': 'off',
  },
}
