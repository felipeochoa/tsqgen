// https://www.postgresql.org/docs/current/sql-syntax-lexical.html


export interface Serializable {
    serialize(): Token[];
}

export function unlex(tokens: Token[]): string {
    return tokens.map((token): string => {
        switch (token.type) {
            case 'KeyWord':
            case 'Identifier':
                return token.value;
            case 'Literal':
                return `'${token.value}'`;
            case 'Operator':
                return token.value;
            case 'SpecialCharacter':
                return token.value;
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
    | { type: 'KeyWord', value: KeyWord }
    | { type: 'Identifier', value: string }
    | { type: 'Literal', value: string }
    | { type: 'Operator', value: string }
    | { type: 'SpecialCharacter', value: SpecialCharacter };

export const keyWord = (value: KeyWord): Token => ({type: 'KeyWord', value});
export const identifier = (value: string): Token => ({type: 'Identifier', value});
export const literal = (value: string): Token => ({type: 'Literal', value});
export const operator = (value: string): Token => ({type: 'Operator', value});
export const specialCharacter = (value: SpecialCharacter): Token => ({type: 'SpecialCharacter', value});

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
