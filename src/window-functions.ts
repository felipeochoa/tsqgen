import { Expression, PartialWindowCall } from './expression';

// https://www.postgresql.org/docs/current/functions-window.html

type Defined<T> = T extends undefined ? never : T;
type Undefined<T> = T extends undefined ? T : never;

const define = <Args extends any[], Return>(name: string) =>
    (...args: {[I in keyof Args]: Undefined<Args[I]> | Expression<Defined<Args[I]>>}) =>
    new PartialWindowCall<Return>(name, args);


// Aliases for documentation purposes only
type BigInt = number;
type DoublePrecision = number;
type Integer = number;

/** Returns the number of the current row within its partition, counting from 1. */
export const rowNumber = define<[], BigInt>('row_number');

/** Returns the rank of the current row, with gaps; that is, the row_number of the first row in its peer group. */
export const rank = define<[], BigInt>('rank');

/** Returns the rank of the current row, without gaps; this function effectively counts peer groups. */
export const denseRank = define<[], BigInt>('dense_rank');

/**
 * Returns the relative rank of the current row, that is (rank - 1) / (total partition rows - 1). The value thus
 * ranges from 0 to 1 inclusive.
 */
export const percentRank = define<[], DoublePrecision>('percent_rank');

/**
 * Returns the cumulative distribution, that is (number of partition rows preceding or peers with current row) /
 * (total partition rows). The value thus ranges from 1/N to 1.
 */
export const cumeDist = define<[], DoublePrecision>('cume_dist');

/** Returns an integer ranging from 1 to the argument value, dividing the partition as equally as possible. */
export const ntile = define<[num_buckets: Integer], Integer>('ntile');

type LagOrLead =
    <T>(value: Expression<T>, offset?: Expression<Integer>, default_?: Expression<T>) => PartialWindowCall<T>;

/**
 * Returns `value` evaluated at the row that is `offset` rows before the current row within the partition; if there
 * is no such row, instead returns `default` (which must be of a type compatible with value). Both `offset` and
 * `default` are evaluated with respect to the current row. If omitted, `offset` defaults to 1 and `default` to
 * NULL.
 */
export const lag: LagOrLead = define<[any, Integer?, any?], any>('lag');

/**
 * Returns `value` evaluated at the row that is `offset` rows after the current row within the partition; if there
 * is no such row, instead returns `default` (which must be of a type compatible with value). Both `offset` and
 * `default` are evaluated with respect to the current row. If omitted, `offset` defaults to 1 and `default` to
 * NULL.
 */
export const lead: LagOrLead = define<[value: any, offset?: Integer, default_?: any], any>('lead');

type FirstOrLast = <T>(value: Expression<T>) => PartialWindowCall<T>;

/** Returns value evaluated at the row that is the first row of the window frame. */
export const firstValue: FirstOrLast = define<[value: any], any>('first_value');

/** Returns value evaluated at the row that is the last row of the window frame. */
export const lastValue: FirstOrLast = define<[value: any], any>('last_value');

/**
 * Returns value evaluated at the row that is the n'th row of the window frame (counting from 1); returns NULL if
 * there is no such row.
 */
export const nthValue: <T>(value: Expression<T>, n: Expression<Integer>) => PartialWindowCall<T>
    = define<[value: any, n: Integer], any>('nth_value');
