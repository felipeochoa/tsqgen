import {
    Aggregate, Expression, JsonArrayAgg, JsonObjectAgg, OrderArg, OrderedSetAggregate, isExpression,
} from './expression';
import { Json, Jsonb, MultiRange, Range, Xml } from './types';

type Defined<T> = T extends undefined ? never : T;
type Undefined<T> = T extends undefined ? T : never;

const define = <Args extends unknown[], Return>(name: string) =>
    (...args: {[I in keyof Args]: Undefined<Args[I]> | Expression<Defined<Args[I]>>}) =>
        new Aggregate<Return>(name, args);

// https://www.postgresql.org/docs/current/functions-aggregate.html#FUNCTIONS-AGGREGATE-TABLE

/** Returns an arbitrary value from the non-null input values. */
export const anyValue: <T>(t: Expression<T>) => Expression<T> = define<[unknown], unknown>('any_value');

/**
 * Collects all the input values, including nulls, into an array. Concatenates all the input arrays into an array
 * of one higher dimension. (Array inputs must all have the same dimensionality, and cannot be empty or null.)
 */
export const arrayAgg: <T>(t: Expression<T>) => Expression<T[]> = define<[unknown], unknown[]>('array_agg');

/** Computes the average (arithmetic mean) of all the non-null input values. */
export const avg = define<[number], number>('avg');

/** Computes the bitwise AND of all non-null input values. */
export const bitAnd = define<[number], number>('bit_and');

/** Computes the bitwise OR of all non-null input values. */
export const bitOr = define<[number], number>('bit_or');

/**
 * Computes the bitwise exclusive OR of all non-null input values. Can be useful as a checksum for an unordered set
 * of values.
 */
export const bitXor = define<[number], number>('bit_xor');

/** Returns true if all non-null input values are true, otherwise false. */
export const boolAnd = define<[boolean], boolean>('bool_and');

/** Returns true if any non-null input value is true, otherwise false. */
export const boolOr = define<[boolean], boolean>('bool_or');

/**
 * The zero-argument form corresponds to COUNT(*) which computes the number of input rows.
 * The one-argument form computes the number of input rows in which the input value is not null.
 */
export const count = define<[unknown?], bigint>('count');

/** This is the SQL standard's equivalent to bool_and. */
export const every = define<[boolean], boolean>('every');

/**
 * Collects all the input values, including nulls, into a JSON array. Values are converted to JSON as per to_json
 * or to_jsonb.
 */
export const jsonAgg = define<[unknown], Json>('json_agg');
export const jsonbAgg = define<[unknown], Jsonb>('jsonb_agg');

export const jsonObjectAgg = <K, V>(key: Expression<K>, value: Expression<V>): JsonObjectAgg<K, V> =>
    new JsonObjectAgg(key, value);

/**
 * Collects all the key/value pairs into a JSON object. Key arguments are coerced to text; value arguments are
 * converted as per to_json or to_jsonb. Values can be null, but keys cannot.
 */
export const jsonObjectAggLegacy = define<[key: unknown, value: unknown], Json>('json_object_agg');
export const jsonbObjectAgg = define<[key: unknown, value: unknown], Jsonb>('jsonb_object_agg');

/**
 * Collects all the key/value pairs into a JSON object. Key arguments are coerced to text; value arguments are
 * converted as per to_json or to_jsonb. The key can not be null. If the value is null then the entry is skipped,
 */
export const jsonObjectAggStrict = define<[key: unknown, value: unknown], Json>('json_object_agg_strict');
export const jsonbObjectAggStrict = define<[key: unknown, value: unknown], Jsonb>('jsonb_object_agg_strict');

/**
 * Collects all the key/value pairs into a JSON object. Key arguments are coerced to text; value arguments are
 * converted as per to_json or to_jsonb. Values can be null, but keys cannot. If there is a duplicate key an error
 * is thrown.
 */
export const jsonObjectAggUnique = define<[key: unknown, value: unknown], Json>('json_object_agg_unique');
export const jsonbObjectAggUnique = define<[key: unknown, value: unknown], Jsonb>('jsonb_object_agg_unique');

/**
 * Aggregate values into a JSON array. If ABSENT ON NULL is specified, any NULL values are omitted. If ORDER BY is
 * specified, the elements will appear in the array in that order rather than in the input order.
 */
export const jsonArrayAgg = <T>(value: Expression<T>): JsonArrayAgg<T> => new JsonArrayAgg(value);

/**
 * Collects all the key/value pairs into a JSON object. Key arguments are coerced to text; value arguments are
 * converted as per to_json or to_jsonb. The key can not be null. If the value is null then the entry is
 * skipped. If there is a duplicate key an error is thrown.
 */
export const jsonObjectAggUniqueStrict
    = define<[key: unknown, value: unknown], Json>('json_object_agg_unique_strict');
export const jsonbObjectAggUniqueStrict
    = define<[key: unknown, value: unknown], Jsonb>('jsonb_object_agg_unique_strict');

/**
 * Computes the maximum of the non-null input values. Available for any numeric, string, date/time, or enum type,
 * as well as inet, interval, money, oid, pg_lsn, tid, xid8, and arrays of any of these types.
 */
export const max = define<[number], number>('max');

/**
 * Computes the minimum of the non-null input values. Available for any numeric, string, date/time, or enum type,
 * as well as inet, interval, money, oid, pg_lsn, tid, xid8, and arrays of any of these types.
 */
export const min = define<[number], number>('min');

/** Computes the union of the non-null input values. */
export const rangeAgg: <T>(r: Expression<Range<T>>) => Expression<MultiRange<T>>
    = define<[Range<unknown>], MultiRange<unknown>>('range_agg');

/** Computes the intersection of the non-null input values. */
export const rangeIntersectAgg: <T>(r: Expression<Range<T>>) => Expression<Range<T>>
    = define<[Range<unknown>], Range<unknown>>('range_intersect_agg');

/**
 * Collects all the input values, skipping nulls, into a JSON array. Values are converted to JSON as per to_json or
 * to_jsonb.
 */
export const jsonAggStrict = define<[unknown], Json>('json_agg_strict');
export const jsonbAggStrict = define<[unknown], Jsonb>('jsonb_agg_strict');

/**
 * Concatenates the non-null input values into a string. Each value after the first is preceded by the
 * corresponding delimiter (if it's not null).
 */
export const stringAgg
    = <T extends Buffer | string>(value: Expression<T>, delimiter: Expression<T>): Expression<T> =>
        new Aggregate<T>('string_agg', [value, delimiter]);

/** Computes the sum of the non-null input values. */
export const sum = define<[number], number>('sum');

/** Concatenates the non-null XML input values. */
export const xmlagg = define<[Xml], Xml>('xmlagg');

// https://www.postgresql.org/docs/current/functions-aggregate.html#FUNCTIONS-AGGREGATE-STATISTICS-TABLE

/** Computes the correlation coefficient. */
export const corr = define<[Y: number, X: number], number>('corr');

/** Computes the population covariance. */
export const covarPop = define<[Y: number, X: number], number>('covar_pop');

/** Computes the sample covariance. */
export const covarSamp = define<[Y: number, X: number], number>('covar_samp');

/** Computes the average of the independent variable, sum(X)/N. */
export const regrAvgx = define<[Y: number, X: number], number>('regr_avgx');

/** Computes the average of the dependent variable, sum(Y)/N. */
export const regrAvgy = define<[Y: number, X: number], number>('regr_avgy');

/** Computes the number of rows in which both inputs are non-null. */
export const regrCount = define<[Y: number, X: number], number>('regr_count');

/** Computes the y-intercept of the least-squares-fit linear equation determined by the (X, Y) pairs. */
export const regrIntercept = define<[Y: number, X: number], number>('regr_intercept');

/** Computes the square of the correlation coefficient. */
export const regrR2 = define<[Y: number, X: number], number>('regr_r2');

/** Computes the slope of the least-squares-fit linear equation determined by the (X, Y) pairs. */
export const regrSlope = define<[Y: number, X: number], number>('regr_slope');

/** Computes the “sum of squares” of the independent variable, sum(X^2) - sum(X)^2/N. */
export const regrSxx = define<[Y: number, X: number], number>('regr_sxx');

/** Computes the “sum of products” of independent times dependent variables, sum(X*Y) - sum(X) * sum(Y)/N. */
export const regrSxy = define<[Y: number, X: number], number>('regr_sxy');

/** Computes the “sum of squares” of the dependent variable, sum(Y^2) - sum(Y)^2/N. */
export const regrSyy = define<[Y: number, X: number], number>('regr_syy');

/** This is a historical alias for stddev_samp. */
export const stddev = define<[number], number>('stddev');

/** Computes the population standard deviation of the input values. */
export const stddevPop = define<[number], number>('stddev_pop');

/** Computes the sample standard deviation of the input values. */
export const stddevSamp = define<[number], number>('stddev_samp');

/** This is a historical alias for var_samp. */
export const variance = define<[number], number>('variance');

/** Computes the population variance of the input values (square of the population standard deviation). */
export const varPop = define<[number], number>('var_pop');

/** Computes the sample variance of the input values (square of the sample standard deviation). */
export const varSamp = define<[number], number>('var_samp');

// https://www.postgresql.org/docs/current/functions-aggregate.html#FUNCTIONS-ORDEREDSET-TABLE

class PartialOrderedSetAggregate<Args extends unknown[], Return> {
    constructor(private functionName: string, private args: Expression<unknown>[]) {}

    withinGroupOrderBy(elts: {[I in keyof Args]: OrderArg<Args[I]> | Expression<Args[I]>}) {
        const orderBy = elts.map(elt => isExpression(elt) ? {expr: elt} : elt);
        return new OrderedSetAggregate<Return>(this.functionName, this.args, orderBy);
    }
}

/**
 * Computes the mode, the most frequent value of the aggregated argument (arbitrarily choosing the first one if
 * there are multiple equally-frequent values). The aggregated argument must be of a sortable type.
 */
export const mode = <T>() => new PartialOrderedSetAggregate<[T], T>('mode', []);

/**
 * Computes continuous percentiles, with two variants, depending on whether the initial argument is a number or an
 * array.
 *
 * Variant 1 (single number): Computes the continuous percentile, a value corresponding to the specified fraction
 * within the ordered set of aggregated argument values. This will interpolate between adjacent input items if
 * needed.
 *
 * Variant 2 (array): Computes multiple continuous percentiles. The result is an array of the same dimensions as
 * the fractions parameter, with each non-null element replaced by the (possibly interpolated) value corresponding
 * to that percentile.
 */
export function percentileCont(fraction: Expression<number>): PartialOrderedSetAggregate<[number], number>;
export function percentileCont(fractions: Expression<number[]>): PartialOrderedSetAggregate<[number], number[]>;
export function percentileCont(fractions: Expression<unknown>): PartialOrderedSetAggregate<[number], unknown> {
    return new PartialOrderedSetAggregate<[number], unknown>('percentile_cont', [fractions]);
}

/**
 * Computes discrete percentiles, with two variants, depending on whether the initial argument is a number or an
 * array.
 *
 * Variant 1 (single number): Computes the discrete percentile, the first value within the ordered set of
 * aggregated argument values whose position in the ordering equals or exceeds the specified fraction. The
 * aggregated argument must be of a sortable type.
 *
 * Variant 2 (array): Computes multiple discrete percentiles. The result is an array of the same dimensions as the
 * fractions parameter, with each non-null element replaced by the input value corresponding to that
 * percentile. The aggregated argument must be of a sortable type.
 */
export function percentileDisc<T>(fraction: Expression<number>): PartialOrderedSetAggregate<[T], T>;
export function percentileDisc<T>(fractions: Expression<number[]>): PartialOrderedSetAggregate<[T], T[]>;
export function percentileDisc<T>(fractions: Expression<unknown>): PartialOrderedSetAggregate<[T], unknown> {
    return new PartialOrderedSetAggregate<[T], unknown>('percentile_disc', [fractions]);
}

// https://www.postgresql.org/docs/current/functions-aggregate.html#FUNCTIONS-HYPOTHETICAL-TABLE
/**
 * Computes the rank of the hypothetical row, with gaps; that is, the row number of the first row in its peer
 * group.
 */
export const rank = <T extends unknown[]>(...args: {[I in keyof T]: Expression<T[I]>}) =>
    new PartialOrderedSetAggregate<T, number>('rank', args);

/** Computes the rank of the hypothetical row, without gaps; this function effectively counts peer groups. */
export const denseRank = <T extends unknown[]>(...args: {[I in keyof T]: Expression<T[I]>}) =>
    new PartialOrderedSetAggregate<T, number>('dense_rank', args);

/**
 * Computes the relative rank of the hypothetical row, that is (rank - 1) / (total rows - 1). The value thus ranges
 * from 0 to 1 inclusive.
 */
export const percentRank = <T extends unknown[]>(...args: {[I in keyof T]: Expression<T[I]>}) =>
    new PartialOrderedSetAggregate<T, number>('percent_rank', args);

/**
 * Computes the cumulative distribution, that is (number of rows preceding or peers with hypothetical row) / (total
 * rows). The value thus ranges from 1/N to 1.
 */
export const cumeDist = <T extends unknown[]>(...args: {[I in keyof T]: Expression<T[I]>}) =>
    new PartialOrderedSetAggregate<T, number>('cume_dist', args);

// https://www.postgresql.org/docs/current/functions-aggregate.html#FUNCTIONS-GROUPING-TABLE

/**
 * Returns a bit mask indicating which GROUP BY expressions are not included in the current grouping set. Bits are
 * assigned with the rightmost argument corresponding to the least-significant bit; each bit is 0 if the
 * corresponding expression is included in the grouping criteria of the grouping set generating the current result
 * row, and 1 if it is not included.
 */
export const grouping = define<unknown[], number>('grouping');
