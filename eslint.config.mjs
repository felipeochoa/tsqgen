/* global process */
import eslint from '@eslint/js';
import stylistic from '@stylistic/eslint-plugin';
import tseslint from 'typescript-eslint';

const lintWithTypes = process.env.LINT_WITH_TYPES === 'true';

export default tseslint.config(
    {ignores: ["build/*"]},
    eslint.configs.recommended,
    tseslint.configs.recommended,
    tseslint.configs.strict,
    stylistic.configs.customize({
        indent: 4,
        semi: true,
        jsx: false,
    }),
    {
        rules: {
            '@stylistic/arrow-parens': 'off',
            '@stylistic/block-spacing': 'off',
            '@stylistic/brace-style': 'off',
            '@stylistic/indent': ['error', 4, {
                ignoredNodes: [
                    // Ignore indent when the body brace is on a newline
                    'MethodDefinition > FunctionExpression > BlockStatement.body',
                ],
                ArrayExpression: 'first',
                CallExpression: {arguments: 'first'},
                FunctionDeclaration: {parameters: 'first'},
                FunctionExpression: {parameters: 'first'},
                SwitchCase: 1,
                flatTernaryExpressions: true,
            }],
            '@stylistic/indent-binary-ops': 'off',
            '@stylistic/lines-between-class-members': 'off',
            '@stylistic/max-len': ['error', {
                code: 115,
                tabWidth: 4,
                ignoreStrings: false,
                ignoreRegExpLiterals: false,
            }],
            '@stylistic/multiline-ternary': 'off',
            '@stylistic/object-curly-spacing': 'off',
            '@stylistic/quote-props': ['error', 'as-needed'],
            '@stylistic/quotes': 'off',

            '@typescript-eslint/consistent-type-assertions': ['error', {
                assertionStyle: 'as',
                objectLiteralTypeAssertions: 'never',
            }],
            '@typescript-eslint/consistent-type-definitions': 'off',
            // Disable methods which get weak bivarant typing
            '@typescript-eslint/method-signature-style': ['error', 'property'],
            '@typescript-eslint/naming-convention': [
                'error',
                ...[
                    // parameterProperty, typeMethod, typeProperty
                    [['class', 'enum', 'enumMember', 'interface', 'typeAlias', 'typeParameter'],
                     ['PascalCase']],

                    [['classMethod', 'classProperty', 'objectLiteralMethod', 'parameter'],
                     ['camelCase']],

                    ['variable', ['camelCase', 'UPPER_CASE']],
                    ['function', ['camelCase', 'PascalCase']],
                    ['import', ['camelCase', 'PascalCase', 'UPPER_CASE']],

                ].map(([selector, format]) =>
                    ({selector, format, leadingUnderscore: 'allowSingleOrDouble', trailingUnderscore: 'forbid'})),
            ],
            "@typescript-eslint/no-explicit-any": 'off',
            "@typescript-eslint/no-non-null-assertion": 'off',
            "@typescript-eslint/no-unused-vars": ['error', {
                args: 'all',
                argsIgnorePattern: '^_',
                destructuredArrayIgnorePattern: '^_',
                varsIgnorePattern: '^_',
            }],
            "@typescript-eslint/no-unsafe-type-assertion": lintWithTypes ? "error" : 'off',
            "@typescript-eslint/prefer-readonly": lintWithTypes ? 'error' : 'off',

            'arrow-body-style': 'off',
            curly: 'off',
            eqeqeq: ['error', 'always', {null: 'ignore'}],
            'max-classes-per-file': 'off',
            'no-bitwise': 'off',
            'no-fallthrough': 'off', // This is better handled by typescript
            'no-unused-expressions': ['error', {
                allowShortCircuit: true,
                allowTernary: true,
            }],
            'sort-keys': 'off',
            'sort-imports': ['error', {
                ignoreCase: false,
                ignoreDeclarationSort: true,
                ignoreMemberSort: false,
            }],
        },
    },
);
