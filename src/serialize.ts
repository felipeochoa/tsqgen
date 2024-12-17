// TODO: Instead of interpolating strings, generate a token stream from Serializable and then "unlex" the tokens.
// https://www.postgresql.org/docs/current/sql-syntax-lexical.html

export type Result<T> = [T, unknown[]];

export function collectParams<T>(result: Result<T>, into: unknown[]): T {
    const [ret, ps] = result;
    into.push(...ps);
    return ret;
}

export interface Serializable {
    serialize(): Result<string>;
}

export type Token =
    | KeyWord
    | Identifier
    | QuotedIdentifier
    | Literal
    | SpecialCharacter;

// https://www.postgresql.org/docs/current/sql-keywords-appendix.html
type KeyWord = never;

type Identifier = never;
type QuotedIdentifier = never;
type Literal = never;
type SpecialCharacter = never;
