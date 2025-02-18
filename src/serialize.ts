import * as quote from './quote';
import { assertNever } from './utils';

function needsQuoting(identifier: string): boolean {
    return !/^[a-zA-Z_][a-zA-Z0-9_$]*$/.test(identifier)
        || isReservedKeyword(identifier.toUpperCase());
}

function isReservedKeyword(identifier: string): boolean {
    const keywords = new Set([
        'ABSENT', 'ALL', 'AND', 'ANY', 'ARRAY', 'AS', 'ASC', 'BETWEEN', 'CAST',
        'CROSS', 'JOIN', 'CUBE', 'DESC', 'DISTINCT', 'EXCEPT', 'FILTER', 'FOR',
        'FROM', 'FULL', 'GROUP', 'BY', 'HAVING', 'INNER', 'INTERSECT', 'JOIN',
        'LATERAL', 'LEFT', 'LIMIT', 'NULL', 'OFFSET', 'ON', 'ORDER', 'OVER',
        'RIGHT', 'SELECT', 'UNION', 'UNIQUE', 'UPDATE', 'USING', 'WHERE', 'WITH',
    ]);
    return keywords.has(identifier);
}

// https://www.postgresql.org/docs/current/sql-syntax-lexical.html

export interface Serializable {
    serialize: () => Token[];
}

export function unlex(tokens: Token[]): string {
    return tokens.reduce((acc, token, index) => {
        const nextToken = tokens[index + 1];
        const result = unlex1(token);

        const needsSpaceAfter = nextToken && !(
            (token.type === 'SpecialCharacter' && (token.value === '(' || token.value === '['))
            || (token.type === 'Identifier' && nextToken.type === 'SpecialCharacter' && nextToken.value === '(')
            || (token.type === 'KeyWord' && ['CAST', 'ARRAY', 'ANY', 'ALL'].includes(token.value))
            || (nextToken.type === 'SpecialCharacter' && (nextToken.value === ')' || nextToken.value === ']'))
            || (nextToken.type === 'SpecialCharacter' && nextToken.value === ',')
        );

        return acc + result + (needsSpaceAfter ? ' ' : '');
    }, '');
}

function unlex1(token: Token) {
    switch (token.type) {
        case 'KeyWord':
            return token.value;
        case 'Identifier':
            if (token.forceQuote) return quote.identifier(token.value);
            return needsQuoting(token.value)
                ? quote.identifier(token.value)
                : token.value;
        case 'Literal': {
            const value = token.value;
            if (typeof value === 'string') {
                return quote.string(value);
            } else if (value === null || typeof value === 'boolean') {
                return '' + value;
            } else if (typeof value === 'number') {
                return isFinite(value) ? value.toString() : quote.string(value.toString());
            }
            assertNever(value, 'Unexpected literal of type ' + typeof value);
        }
        case 'Operator':
            return quote.operator(token.value);
        case 'SpecialCharacter':
            return token.value;
        case 'ColumnReference':
            return `${quote.identifier(token.tableName)}.${quote.identifier(token.columnName)}`;
    }
}

export function commaSeparate(args: readonly Token[][]): Token[] {
    if (args.length === 0) return [];
    const ret = [...args[0]];
    for (let i = 1; i < args.length; i++) {
        ret.push({type: 'SpecialCharacter', value: ','}, ...args[i]);
    }
    return ret;
}

export type Token =
    | {type: 'KeyWord'; value: KeyWord}
    | {type: 'Identifier'; value: string; forceQuote: boolean}
    | {type: 'Literal'; value: string | number | boolean | null}
    | {type: 'Operator'; value: string}
    | {type: 'SpecialCharacter'; value: SpecialCharacter}
    | {type: 'ColumnReference'; tableName: string; columnName: string};

export const keyWord = (value: KeyWord): Token => ({type: 'KeyWord', value});
export const identifier = (value: string, forceQuote?: boolean): Token =>
    ({type: 'Identifier', value, forceQuote: forceQuote ?? false});
export const literal = (value: string | number | boolean | null): Token => ({type: 'Literal', value});
export const operator = (value: string): Token => ({type: 'Operator', value});
export const specialCharacter = (value: SpecialCharacter): Token => ({type: 'SpecialCharacter', value});
export const columnReference = (tableName: string, columnName: string): Token => ({
    type: 'ColumnReference',
    tableName,
    columnName,
});

type SpecialCharacter = '(' | ')' | '[' | ']' | '*' | ',' | '.' | ':';

// TODO: Cross-check against https://www.postgresql.org/docs/current/sql-keywords-appendix.html
type KeyWord =
    | 'ABSENT'
    | 'ALL'
    | 'AND'
    | 'ANY'
    | 'ARRAY'
    | 'AS'
    | 'ASC'
    | 'BETWEEN'
    | 'CAST'
    | 'CROSS JOIN'
    | 'CUBE'
    | 'CURRENT ROW'
    | 'DESC'
    | 'DISTINCT'
    | 'EXCEPT'
    | 'EXCLUDE CURRENT ROW'
    | 'EXCLUDE GROUP'
    | 'EXCLUDE NO OTHERS'
    | 'EXCLUDE TIES'
    | 'FETCH NEXT'
    | 'FILTER'
    | 'FOLLOWING'
    | 'FOR'
    | 'FROM'
    | 'FULL'
    | 'GROUP BY'
    | 'GROUPING SETS'
    | 'GROUPS'
    | 'HAVING'
    | 'INNER'
    | 'INTERSECT'
    | 'JOIN'
    | 'KEY SHARE'
    | 'LATERAL'
    | 'LEFT'
    | 'LIMIT'
    | 'NO KEY UPDATE'
    | 'NOWAIT'
    | 'NULL'
    | 'NULLS FIRST'
    | 'NULLS LAST'
    | 'NULLS'
    | 'OF'
    | 'OFFSET'
    | 'ON'
    | 'ONLY'
    | 'ORDER BY'
    | 'OVER'
    | 'PARTITION BY'
    | 'PRECEDING'
    | 'RANGE'
    | 'RIGHT'
    | 'ROLLUP'
    | 'ROWS'
    | 'SELECT'
    | 'SHARE'
    | 'SKIP LOCKED'
    | 'UNBOUNDED FOLLOWING'
    | 'UNBOUNDED PRECEDING'
    | 'UNION'
    | 'UNIQUE KEYS'
    | 'UPDATE'
    | 'USING'
    | 'WHERE'
    | 'WINDOW'
    | 'WITH ORDINALITY'
    | 'WITH TIES'
    | 'WITH'
    | 'WITHIN GROUP'
    | 'WITHOUT';
