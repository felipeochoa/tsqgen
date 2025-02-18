import {
    Serializable, Token, commaSeparate, identifier, keyWord, literal, operator, specialCharacter,
} from './serialize';
import { Subquery } from './select-types';
import { Json, SQL, SqlType } from './types';

// Expression syntax taken from https://www.postgresql.org/docs/current/sql-expressions.html

export type Order = {key: 'ASC'} | {key: 'DESC'} | {key: 'USING'; op: string};
export interface OrderArg<T = unknown> {
    expr: FinalExpression<T>;
    order?: Order;
    nulls?: 'NULLS FIRST' | 'NULLS LAST';
}

declare const __brand: unique symbol;
const expressionTag = Symbol();

/** An Expression that won't be further manipulated. */
interface FinalExpression<T> extends Serializable {
    // This type is needed because Expression is invariant in its type, but we sometimes want a covariant
    // expression purely for type-checking purposes
    readonly [expressionTag]: true;
    readonly [__brand]?: T; // Needed to make typescript actually check that Expression types line up
}

export type UnknownExpr = FinalExpression<unknown>;

interface SingleTypeSubquery<Value> extends Serializable {
    /** Unused field to track subquery type. */
    _tuple: Record<string, FinalExpression<Value>>;
}

export interface Expression<T> {
    [expressionTag]: true;
    [__brand]?: T; // Needed to make typescript actually check that Expression types line up
    isNull: () => Expression<boolean>;
    isNotNull: () => Expression<boolean>;
    or: (other: Expression<boolean>) => Expression<boolean>;
    and: (other: Expression<boolean>) => Expression<boolean>;
    isDistinctFrom: (other: Expression<T>) => Expression<boolean>;
    isNotDistinctFrom: (other: Expression<T>) => Expression<boolean>;
    eq: (other: Expression<T>) => Expression<boolean>;
    ne: (other: Expression<T>) => Expression<boolean>;
    lt: (this: Expression<T & number>, other: Expression<T>) => Expression<boolean>;
    le: (this: Expression<T & number>, other: Expression<T>) => Expression<boolean>;
    gt: (this: Expression<T & number>, other: Expression<T>) => Expression<boolean>;
    ge: (this: Expression<T & number>, other: Expression<T>) => Expression<boolean>;
    like: (this: Expression<T & string>, other: Expression<string>) => Expression<boolean>;
    ilike: (this: Expression<T & string>, other: Expression<string>) => Expression<boolean>;
    collate: (this: Expression<T & string>, collation: string) => Expression<string>;
    castAs: <T2>(type: SqlType<T2>) => Expression<T2>;
    in: ((...values: Expression<T>[]) => Expression<boolean>)
        & ((subquery: SingleTypeSubquery<T>) => Expression<boolean>);
    notIn: ((...values: Expression<T>[]) => Expression<boolean>)
           & ((subquery: SingleTypeSubquery<T>) => Expression<boolean>);
    any: (operator: string, array: Expression<T[]> | SingleTypeSubquery<T>) => Expression<boolean>;
    all: (operator: string, array: Expression<T[]> | SingleTypeSubquery<T>) => Expression<boolean>;

    asc: (nulls?: 'NULLS FIRST' | 'NULLS LAST') => OrderArg<T>;
    desc: (nulls?: 'NULLS FIRST' | 'NULLS LAST') => OrderArg<T>;
    using: (op: string, nulls?: 'NULLS FIRST' | 'NULLS LAST') => OrderArg<T>;

    serialize: () => Token[];
}

const assertType = <T>(t: T) => t;

export const isExpression = <T, O extends object>(x: Expression<T> | O):
    x is Expression<T> => Boolean(expressionTag in x
                                  && assertType<{readonly [expressionTag]?: true}>(x)[expressionTag]);

export const isFinalExpression = <T, O extends object>(x: FinalExpression<T> | O):
    x is FinalExpression<T> => Boolean(expressionTag in x
                                       && assertType<{readonly [expressionTag]?: true}>(x)[expressionTag]);

abstract class BaseExpr<T> implements Expression<T> {
    [expressionTag] = true as const;
    isNull(): Expression<boolean> { return new PostfixExpr(this, 'IS NULL'); }
    isNotNull(): Expression<boolean> { return new PostfixExpr(this, 'IS NOT NULL'); }
    or(other: Expression<boolean>): Expression<boolean> { return new InfixExpr(this, 'OR', other); }
    and(other: Expression<boolean>): Expression<boolean> { return new InfixExpr(this, 'AND', other); }
    isDistinctFrom(other: Expression<T>): Expression<boolean>
        { return new InfixExpr(this, 'IS DISTINCT FROM', other); }
    isNotDistinctFrom(other: Expression<T>): Expression<boolean>
        { return new InfixExpr(this, 'IS NOT DISTINCT FROM', other); }
    eq(other: Expression<T>): Expression<boolean> { return new InfixExpr(this, '=', other); }
    ne(other: Expression<T>): Expression<boolean> { return new InfixExpr(this, '<>', other); }
    lt(this: Expression<T & number>, other: Expression<T>): Expression<boolean>
        { return new InfixExpr(this, '<', other); }
    le(this: Expression<T & number>, other: Expression<T>): Expression<boolean>
        { return new InfixExpr(this, '<=', other); }
    gt(this: Expression<T & number>, other: Expression<T>): Expression<boolean>
        { return new InfixExpr(this, '>', other); }
    ge(this: Expression<T & number>, other: Expression<T>): Expression<boolean>
        { return new InfixExpr(this, '>=', other); }
    like(this: Expression<T & string>, other: Expression<string>): Expression<boolean>
        { return new InfixExpr(this, 'LIKE', other); }
    ilike(this: Expression<T & string>, other: Expression<string>): Expression<boolean>
        { return new InfixExpr(this, 'ILIKE', other); }
    collate(this: Expression<T & string>, collation: string): Expression<string>
        { return new InfixExpr(this, 'COLLATE', new CollationIdentifier(collation)); }

    castAs<T2>(type: SqlType<T2>): Expression<T2>
        { return new Cast(this, type.name); }
    in(subquery: SingleTypeSubquery<T>): Expression<boolean>;
    in(...values: Expression<T>[]): Expression<boolean>;
    in(arg1?: SingleTypeSubquery<T> | Expression<T>, ...rest: Expression<T>[]): Expression<boolean> {
        if (arg1 === undefined) return new MultiOperandExpr(this, 'IN', []);
        if (isExpression(arg1)) return new MultiOperandExpr(this, 'IN', [arg1, ...rest]);
        return new InfixExpr(this, 'IN', new SubqueryExpr(arg1));
    }
    notIn(subquery: SingleTypeSubquery<T>): Expression<boolean>;
    notIn(...values: Expression<T>[]): Expression<boolean>;
    notIn(arg1?: SingleTypeSubquery<T> | Expression<T>, ...rest: Expression<T>[]): Expression<boolean> {
        if (arg1 === undefined) return new MultiOperandExpr(this, 'NOT IN', []);
        if (isExpression(arg1)) return new MultiOperandExpr(this, 'NOT IN', [arg1, ...rest]);
        return new InfixExpr(this, 'NOT IN', new SubqueryExpr(arg1));
    }
    any(operator: string, arrayExpr: Expression<T[]> | SingleTypeSubquery<T>): Expression<boolean> {
        return new AnyOrAllExpr(this, operator, arrayExpr, 'ANY');
    }
    all(operator: string, arrayExpr: Expression<T[]> | SingleTypeSubquery<T>): Expression<boolean> {
        return new AnyOrAllExpr(this, operator, arrayExpr, 'ALL');
    }

    asc(nulls?: 'NULLS FIRST' | 'NULLS LAST'): OrderArg<T>
        { return {expr: this, order: {key: 'ASC'}, nulls}; }
    desc(nulls?: 'NULLS FIRST' | 'NULLS LAST'): OrderArg<T>
        { return {expr: this, order: {key: 'DESC'}, nulls}; }
    using(op: string, nulls?: 'NULLS FIRST' | 'NULLS LAST'): OrderArg<T>
        { return {expr: this, order: {key: 'USING', op}, nulls}; }

    abstract serialize(): Token[];
}

class AnyOrAllExpr<L, T> extends BaseExpr<boolean> {
    constructor(
        private left: Expression<L>,
        private op: string,
        private arrayExpr: Expression<T[]> | SingleTypeSubquery<T>,
        private keyword: 'ANY' | 'ALL',
    ) {
        super();
    }

    serialize(): Token[] {
        return [
            specialCharacter('('),
            ...this.left.serialize(),
            operator(this.op),
            keyWord(this.keyword),
            specialCharacter('('),
            ...(isExpression(this.arrayExpr)
                ? this.arrayExpr.serialize()
                : [specialCharacter('('), ...this.arrayExpr.serialize(), specialCharacter(')')]),
            specialCharacter(')'),
            specialCharacter(')'),
        ];
    }
}

export class SubqueryExpr<Value> extends BaseExpr<Value> {
    constructor(private subquery: SingleTypeSubquery<Value>) {
        super();
    }

    serialize(): Token[] {
        return [
            specialCharacter('('),
            ...this.subquery.serialize(),
            specialCharacter(')'),
        ];
    }
}

class Constant<T extends string | number | boolean | null> extends BaseExpr<Widen<T>> {
    constructor(private value: T) {
        super();
    }

    serialize(): Token[] {
        return [literal(this.value)];
    }
}

type Widen<T> = T extends string ? string : T extends number ? number : T extends boolean ? boolean : T;
type StaticValuesOnly<T>
    = T extends string ? (string extends T ? never : T)
    : T extends number ? (number extends T ? never : T)
    : T extends boolean ? (boolean extends T ? never : T)
    : T extends null ? T
    : never;

export const constant = <T extends string | number | boolean | null>(value: StaticValuesOnly<T>): Expression<Widen<T>> =>
    new Constant(value as T);

export const number = (value: number): Expression<number> => new Constant(value);

class CollationIdentifier extends BaseExpr<string> {
    constructor(private name: string) {
        super();
    }

    serialize(): Token[] {
        return [identifier(this.name, true)];
    }
}

class PrefixExpr<Op, Result> extends BaseExpr<Result> {
    constructor(private op: string, private operand: Expression<Op>) {
        super();
    }

    serialize(): Token[] {
        const operand = this.operand.serialize();
        return [
            specialCharacter('('),
            operator(this.op),
            ...operand,
            specialCharacter(')'),
        ];
    }
}

class PostfixExpr<Op, T> extends BaseExpr<T> {
    constructor(private operand: Expression<Op>, private op: string) {
        super();
    }

    serialize(): Token[] {
        const operand = this.operand.serialize();
        return [
            specialCharacter('('),
            ...operand,
            operator(this.op),
            specialCharacter(')'),
        ];
    }
}

class InfixExpr<L, R, T> extends BaseExpr<T> {
    constructor(private left: Expression<L>, private op: string, private right: Expression<R>) {
        super();
    }

    serialize(): Token[] {
        return [
            specialCharacter('('),
            ...this.left.serialize(),
            operator(this.op),
            ...this.right.serialize(),
            specialCharacter(')'),
        ];
    }
}

class MultiOperandExpr<L, R, T> extends BaseExpr<T> {
    // Like InfixExpr, but can take multiple operands on the right
    constructor(private left: Expression<L>, private op: string, private right: Expression<R>[]) {
        super();
    }

    serialize(): Token[] {
        const left = this.left.serialize();
        const right = this.right.map(e => e.serialize());
        return [
            specialCharacter('('),
            ...left,
            operator(this.op),
            specialCharacter('('),
            ...commaSeparate(right),
            specialCharacter(')'),
            specialCharacter(')'),
        ];
    }
}

class FuncExpr<T> extends BaseExpr<T> {
    constructor(private functionName: string, private args: FinalExpression<unknown>[]) {
        super();
    }

    serialize(): Token[] {
        const fn = this.functionName;
        const values = this.args.map(arg => arg.serialize());
        return [
            identifier(fn),
            specialCharacter('('),
            ...commaSeparate(values),
            specialCharacter(')'),
        ];
    }
}

export const func = <T>(name: string, args: FinalExpression<unknown>[]): Expression<T> => new FuncExpr(name, args);

export const not = (arg: Expression<boolean>): Expression<boolean> => new PrefixExpr('not', arg);

/** Aggregates not using WITHIN GROUP. */
export class Aggregate<T> extends BaseExpr<T> {
    constructor(private functionName: string, private args: UnknownExpr[], private doDistinct?: boolean,
                private order?: OrderArg[], private filter?: Expression<boolean>) {
        if (args.length === 0) {
            if (order) throw new Error(`${functionName}(*) cannot specify an ORDER BY clause`);
            if (doDistinct) throw new Error(`${functionName}(*) cannot use DISTINCT`);
        }
        super();
    }

    distinct() {
        return new Aggregate(this.functionName, this.args, true, this.order, this.filter);
    }

    orderBy(order: OrderArg[]) {
        return new Aggregate(this.functionName, this.args, this.doDistinct, order, this.filter);
    }

    filterWhere(filter: Expression<boolean>): Expression<T> {
        return new Aggregate(this.functionName, this.args, this.doDistinct, this.order, filter);
    }

    // TODO: Add an over method to convert the aggregate into a window function: In addition to these functions,
    // any built-in or user-defined ordinary aggregate (i.e., not ordered-set or hypothetical-set aggregates) can
    // be used as a window function... Aggregate functions act as window functions only when an OVER clause follows
    // the call https://www.postgresql.org/docs/current/functions-window.html

    serialize(): Token[] {
        const fn = this.functionName;
        const args = serializeArgs(this.args);
        const filter = serializeFilterWhere(this.filter);
        const distinct = this.doDistinct ? [keyWord('DISTINCT')] : [];
        const orderBy = this.order ? serializeOrderBy(this.order) : [];
        return [
            identifier(fn),
            specialCharacter('('),
            ...distinct,
            ...commaSeparate(args),
            ...orderBy,
            specialCharacter(')'),
            ...filter,
        ];
    }
}

/**  json_arrayagg has unique syntax as an aggregate */
interface JsonObjectAggOnNull extends Expression<Json> {
    absentOnNull: () => JsonObjectAggUniqueKeys;
    nullOnNull: () => JsonObjectAggUniqueKeys;
}

interface JsonObjectAggUniqueKeys extends Expression<Json> {
    withUniqueKeys: () => Expression<Json>;
    withoutUniqueKeys: () => Expression<Json>;
}

export class JsonObjectAgg<K, V> extends BaseExpr<Json> implements JsonObjectAggOnNull, JsonObjectAggUniqueKeys {
    private readonly onNullOption?: 'NULL' | 'ABSENT';
    private readonly uniqueKeys?: 'WITH' | 'WITHOUT';

    constructor(private readonly key: Expression<K>, private readonly value: Expression<V>,
                onNullOption?: 'NULL' | 'ABSENT', uniqueOption?: 'WITH' | 'WITHOUT') {
        super();
        this.onNullOption = onNullOption;
        this.uniqueKeys = uniqueOption;
    }

    absentOnNull(): JsonObjectAggUniqueKeys {
        return new JsonObjectAgg(this.key, this.value, 'ABSENT', this.uniqueKeys);
    }

    nullOnNull(): JsonObjectAggUniqueKeys {
        return new JsonObjectAgg(this.key, this.value, 'NULL', this.uniqueKeys);
    }

    withUniqueKeys(): Expression<Json> {
        return new JsonObjectAgg(this.key, this.value, this.onNullOption, 'WITH');
    }

    withoutUniqueKeys(): Expression<Json> {
        return new JsonObjectAgg(this.key, this.value, this.onNullOption, 'WITHOUT');
    }

    serialize(): Token[] {
        const onNull = this.onNullOption ? [keyWord(this.onNullOption), keyWord('ON'), keyWord('NULL')] : [];
        const unique = this.uniqueKeys ? [keyWord(this.uniqueKeys), keyWord('UNIQUE KEYS')] : [];
        return [
            identifier('json_object_agg'),
            specialCharacter('('),
            ...this.key.serialize(),
            specialCharacter(':'),
            ...this.value.serialize(),
            ...onNull,
            ...unique,
            specialCharacter(')'),
        ];
    }
}

export class JsonArrayAgg<T> extends BaseExpr<Json> {
    constructor(
        private readonly value: Expression<T>,
        private readonly orderByClause?: OrderArg[],
        private readonly onNullOption?: 'NULL' | 'ABSENT',
    ) {
        super();
    }

    orderBy(orderBy: OrderArg[]): Omit<JsonArrayAgg<T>, 'orderBy'> {
        return new JsonArrayAgg(this.value, orderBy, this.onNullOption);
    }

    absentOnNull(): Omit<JsonArrayAgg<T>, 'orderBy' | 'absentOnNull' | 'nullOnNull'> {
        return new JsonArrayAgg(this.value, this.orderByClause, 'ABSENT');
    }

    nullOnNull(): Omit<JsonArrayAgg<T>, 'orderBy' | 'absentOnNull' | 'nullOnNull'> {
        return new JsonArrayAgg(this.value, this.orderByClause, 'NULL');
    }

    serialize(): Token[] {
        const onNull = this.onNullOption ? [keyWord(this.onNullOption), keyWord('ON'), keyWord('NULL')] : [];
        return [
            identifier('json_array_agg'),
            specialCharacter('('),
            ...this.value.serialize(),
            ...(this.orderByClause ? [keyWord('ORDER BY'), ...serializeOrderBy(this.orderByClause)] : []),
            ...onNull,
            specialCharacter(')'),
        ];
    }
}

/** Aggregates using WITHIN GROUP */
export class OrderedSetAggregate<T> extends BaseExpr<T> {
    constructor(private functionName: string, private args: UnknownExpr[], private order: OrderArg[],
                private filter?: Expression<boolean>) {
        super();
    }

    filterWhere(filter: Expression<boolean>): Expression<T> {
        return new OrderedSetAggregate(this.functionName, this.args, this.order, filter);
    }

    serialize(): Token[] {
        const fn = this.functionName;
        const args = serializeArgs(this.args);
        const filter = serializeFilterWhere(this.filter);
        const orderBy = serializeOrderBy(this.order);
        return [
            identifier(fn),
            specialCharacter('('),
            ...commaSeparate(args),
            specialCharacter(')'),
            keyWord('WITHIN GROUP'),
            specialCharacter('('),
            ...orderBy,
            specialCharacter(')'),
            ...filter,
        ];
    }
}

export function agg<T>(name: string, args: Expression<unknown>[]): Aggregate<T>;
export function agg<T>(name: string, args: Expression<unknown>[], _: 'WITHIN GROUP', orderBy: OrderArg[]):
OrderedSetAggregate<T>;
export function agg<T>(name: string, args: Expression<unknown>[], _?: string, orderBy?: OrderArg[]) {
    if (orderBy === undefined) return new Aggregate<T>(name, args);
    return new OrderedSetAggregate<T>(name, args, orderBy);
}

class WindowCall<T> extends BaseExpr<T> {
    // TODO: Support inline definitions
    constructor(private functionName: string, private args: Expression<unknown>[], private over: string,
                private filter?: Expression<boolean>) {
        super();
    }

    serialize(): Token[] {
        const fn = this.functionName;
        const args = serializeArgs(this.args);
        const filter = serializeFilterWhere(this.filter);
        return [
            identifier(fn),
            specialCharacter('('),
            ...(args.length ? commaSeparate(args) : [specialCharacter('*')]),
            specialCharacter(')'),
            ...filter,
            keyWord('OVER'),
            identifier(this.over),
        ];
    }
}

export class PartialWindowCall<T> {
    constructor(private name: string, private args: Expression<unknown>[], private filter?: Expression<boolean>) {}

    filterWhere(filter: Expression<boolean>): Omit<PartialWindowCall<T>, 'filterWhere'> {
        return new PartialWindowCall<T>(this.name, this.args, filter);
    }

    over(window: string): Expression<T> { // TODO: support inline window definitions
        return new WindowCall(this.name, this.args, window, this.filter);
    }
}

class Cast<Orig, NewType> extends BaseExpr<NewType> {
    constructor(private expression: Expression<Orig>, private toType: string) {
        super();
    }

    serialize(): Token[] {
        const expr = this.expression.serialize();
        return [
            keyWord('CAST'),
            specialCharacter('('),
            ...expr,
            keyWord('AS'),
            identifier(this.toType),
            specialCharacter(')'),
        ];
    }
}

class ArrayExpr<T> extends BaseExpr<T[]> {
    constructor(private args: Serializable[]) {
        super();
    }

    serialize(): Token[] {
        return [
            keyWord('ARRAY'),
            specialCharacter('['),
            ...commaSeparate(serializeArgs(this.args)),
            specialCharacter(']'),
        ];
    }
}

export function array<T>(subquery: Subquery<Record<string, Record<string, T>>>): Expression<T[]>;
export function array<T>(...args: Expression<T>[]): Expression<T[]>;
export function array<T>(...args: [Subquery<T>] | Expression<T>[]): Expression<T[]> {
    return new ArrayExpr(args);
}

export const row = <T extends unknown[]>(...args: {[I in keyof T]: Expression<T[I]>}): Expression<T> =>
    new FuncExpr('ROW', args);

export const exists = (subquery: Subquery<unknown>) => new FuncExpr<boolean>('EXISTS', [subquery.scalar()]);

class Field<T> extends BaseExpr<T> {
    constructor(private tableName: string, private name: string) {
        super();
    }

    serialize(): Token[] {
        return [
            identifier(this.tableName),
            specialCharacter('.'),
            identifier(this.name),
        ];
    }
}

export const field = <T>(tableName: string, name: string): Expression<T> => new Field(tableName, name);

class ParameterExpr<T> extends BaseExpr<T> {
    constructor(private index: number) {
        super();
    }

    serialize(): Token[] {
        return [literal('$' + this.index)];
    }
}

type Parameters<T> = ((t: T) => unknown[]) & {[K in keyof T]: Expression<T[K]>};

export function $<T>(types: {[K in keyof T]: SQL<T[K]>}): Parameters<T> {
    const keys = Object.keys(types) as (keyof T)[];
    const ret = Object.assign({}) as {[K in keyof T]: Expression<T[K]>};
    keys.forEach((n, i) => ret[n as keyof T] = new ParameterExpr(i));
    const ret2 = (t: T): unknown[] => keys.map(k => t[k]);
    return Object.assign(ret2, ret);
}

function serializeArgs(args: Serializable[]): Token[][] {
    return args.map(arg => arg.serialize());
}

function serializeFilterWhere(expr: Expression<boolean> | undefined): Token[] {
    if (!expr) return [];
    return [
        keyWord('FILTER'),
        specialCharacter('('),
        keyWord('WHERE'),
        ...expr.serialize(),
        specialCharacter(')'),
    ];
}

function serializeOrderBy(args: OrderArg[]): Token[] {
    return commaSeparate(args.map(({expr, order, nulls}): Token[] => [
        ...expr.serialize(),
        ...(order ? serializeOrder(order) : []),
        ...(nulls ? [keyWord('NULLS'), keyWord(nulls)] : []),
    ]));

    function serializeOrder(order: Order): Token[] {
        return [
            keyWord(order.key),
            ...(order.key === 'USING' ? [identifier(order.op)] : []),
        ];
    }
}
