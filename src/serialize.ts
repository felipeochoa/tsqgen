import { assertNever } from './utils';

// https://www.postgresql.org/docs/current/sql-syntax-lexical.html

export interface Serializable {
    serialize: () => Token[];
}

export function unlex(tokens: Token[]): string {
    return tokens.map((token): string => {
        switch (token.type) {
            case 'KeyWord':
            case 'Identifier':
                return token.value;
            case 'Literal': {
                const value = token.value;
                if (typeof value === 'string') {
                    return `'${value}'`;
                } else if (value === null || typeof value === 'boolean') {
                    return '' + value;
                } else if (typeof value === 'number') {
                    return isFinite(value) ? value.toString() : `'${value}'`;
                }
                assertNever(value, 'Unexpected literal of type ' + typeof value);
            }
            case 'Operator':
                return token.value;
            case 'SpecialCharacter':
                return token.value;
            case 'ColumnReference':
                return `${token.tableName}.${token.columnName}`;
        }
    }).join(' ');
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
    | {type: 'Identifier'; value: string}
    | {type: 'Literal'; value: string | number | boolean | null}
    | {type: 'Operator'; value: string}
    | {type: 'SpecialCharacter'; value: SpecialCharacter}
    | {type: 'ColumnReference'; tableName: string; columnName: string};

export const keyWord = (value: KeyWord): Token => ({type: 'KeyWord', value});
export const identifier = (value: string): Token => ({type: 'Identifier', value});
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
