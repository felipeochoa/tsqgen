// Escape syntax is documented here: https://www.postgresql.org/docs/current/sql-syntax-lexical.html

export function identifier(ident: string) {
    if (/^[a-z]+$/.test(ident)) return ident;
    return `"${ident.replace(/"/g, '""')}"`;
}

export const string = (lit: string) => `'${lit.replace(/'/g, "''")}'`;

export function operator(op: string) {
    if (validOperators.has(op)) return op;
    if (!/[+\-*/<>=~!@#%^&|`?]/.test(op) || /--|\/\*/.test(op))
        throw new Error('Invalid operator: ' + op);
    return op;
}

const validOperators = new Set([
    'AND', 'OR', 'NOT', // https://www.postgresql.org/docs/current/sql-expressions.html#4.2.5
    'LIKE', 'NOT LIKE', 'ILIKE', 'NOT ILIKE', 'SIMILAR TO', 'NOT SIMILAR TO',
    'IS NULL', 'IS NOT NULL',
    'IN', 'NOT IN', 'EXISTS',
    'IS DISTINCT FROM', 'IS NOT DISTINCT FROM',
    'COLLATE',
]);
