module.exports = {
  env: {
    browser: true,
    commonjs: true,
    es2021: true,
  },
  extends: [
    'airbnb-base',
    'plugin:jsonc/base',
    'plugin:jsonc/recommended-with-json5'
  ],
  overrides: [
    {
      env: {
        node: true,
      },
      files: [
        '.eslintrc.{js,cjs}',
      ],
      parserOptions: {
        sourceType: 'script',
      },
      plugins: [
        'json5',
      ],
    },
  ],
  parserOptions: {
    ecmaVersion: 'latest',
  },

  rules: {
    'func-names': 'off', // Anonymous functions have their useful cases
    'no-param-reassign': [2, { props: false }], // We often assign props of an object in a function, and that's generally safe.
  },
};
