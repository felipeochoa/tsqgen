import * as quote from './quote';
import { collectParams, Result, Serializable } from './serialize';
import { Subquery } from './select-types';
import { SQL } from './types';

// Expression syntax taken from https://www.postgresql.org/docs/current/sql-expressions.html

type AnyKey = string | symbol | number;
type SingleTypeSubquery<Value> = Subquery<Partial<Record<AnyKey, Value>>>;

export type Order = {key: 'ASC'} | {key: 'DESC'} | {key: 'USING', op: string};
export type OrderArg<T = unknown> = {expr: Expression<T>, order?: Order, nulls?: 'NULLS FIRST' | 'NULLS LAST'};

declare const __brand: unique symbol;
const expressionTag = Symbol();

export interface Expression<T> {
    [expressionTag]: true;
    [__brand]?: T; // Needed to make typescript actually check that Expression types line up
    isNull(): Expression<boolean>;
    isNotNull(): Expression<boolean>;
    or(other: Expression<boolean>): Expression<boolean>;
    and(other: Expression<boolean>): Expression<boolean>;
    isDistinctFrom(other: Expression<T>): Expression<boolean>;
    isNotDistinctFrom(other: Expression<T>): Expression<boolean>;
    eq(other: Expression<T>): Expression<boolean>;
    lt(this: Expression<T & number>, other: Expression<T>): Expression<boolean>;
    le(this: Expression<T & number>, other: Expression<T>): Expression<boolean>;
    gt(this: Expression<T & number>, other: Expression<T>): Expression<boolean>;
    ge(this: Expression<T & number>, other: Expression<T>): Expression<boolean>;
    like(this: Expression<T & string>, other: Expression<string>): Expression<boolean>;
    ilike(this: Expression<T & string>, other: Expression<string>): Expression<boolean>;
    collate(this: Expression<T & string>, collation: string): Expression<string>;
    castAs<T2>(typeName: string): Expression<T2>;
    in(...values: Expression<T>[]): Expression<boolean>;
    // in(subquery: SingleTypeSubquery<T>): Expression<boolean>; TODO -- this causes lots of type errors
    notIn(...values: Expression<T>[]): Expression<boolean>;
    notIn(subquery: SingleTypeSubquery<T>): Expression<boolean>;
    any(operator: string, array: Expression<T[]> | SingleTypeSubquery<T>): Expression<boolean>;
    all(operator: string, array: Expression<T[]> | SingleTypeSubquery<T>): Expression<boolean>;

    asc(nulls?: 'NULLS FIRST' | 'NULLS LAST'): OrderArg;
    desc(nulls?: 'NULLS FIRST' | 'NULLS LAST'): OrderArg;
    using(op: string, nulls?: 'NULLS FIRST' | 'NULLS LAST'): OrderArg;

    serialize(): Result<string>;
}

export const isExpression = <T, O extends object>(x: Expression<T> | O):
    x is Expression<T> => Boolean((x as {readonly [expressionTag]?: undefined})[expressionTag]);

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
    ne(other: Expression<unknown>): Expression<boolean> { return new InfixExpr(this, '<>', other); }
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
        { return new InfixExpr(this, 'COLLATE', new Identifier(collation)); }
    castAs<T2>(typeName: string): Expression<T2>
        { return new Cast(this, typeName); }
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
        const op = quote.operator(operator) + ' ANY';
        if (isExpression(arrayExpr)) return new MultiOperandExpr(this, op, [arrayExpr]);
        return new InfixExpr(this, op, new SubqueryExpr(arrayExpr));
    }
    all(operator: string, arrayExpr: Expression<T[]> | SingleTypeSubquery<T>): Expression<boolean> {
        const op = quote.operator(operator) + ' ALL';
        if (isExpression(arrayExpr)) return new MultiOperandExpr(this, op, [arrayExpr]);
        return new InfixExpr(this, op, new SubqueryExpr(arrayExpr));
    }

    asc(nulls?: 'NULLS FIRST' | 'NULLS LAST'): OrderArg
        { return {expr: this, order: {key: 'ASC'}, nulls}; }
    desc(nulls?: 'NULLS FIRST' | 'NULLS LAST'): OrderArg
        { return {expr: this, order: {key: 'DESC'}, nulls}; }
    using(op: string, nulls?: 'NULLS FIRST' | 'NULLS LAST'): OrderArg
        { return {expr: this, order: {key: 'USING', op}, nulls}; }

    abstract serialize(): Result<string>;
}

export class SubqueryExpr<Value> extends BaseExpr<Value> {
    constructor(private subquery: SingleTypeSubquery<Value>) {
        super();
    }

    serialize(): Result<string> {
        const [subquery, params] = this.subquery.serialize();
        return [`(${subquery})`, params];
    }
}

class Constant<T> extends BaseExpr<T> {
    constructor(private value: T) {
        super();
    }

    serialize(): Result<string> {
        return [quote.literal(this.value), []];
    }
}

type StaticStringsOnly<T> = T extends string ? (string extends T ? never : T) : T;

export const constant = <T extends string | number | boolean | null>(value: StaticStringsOnly<T>): Expression<T> =>
    new Constant(value);


class Identifier<T> extends BaseExpr<T> {
    constructor(private name: string) {
        super();
    }

    serialize(): Result<string> {
        return [quote.identifier(this.name), []];
    }
}

class PrefixExpr<T> extends BaseExpr<T> {
    constructor(private op: string, private operand: Expression<unknown>) {
        super();
    }

    serialize(): Result<string> {
        const [operand, params] = this.operand.serialize();
        return [`(${this.op} ${operand})`, params];
    }
}

class PostfixExpr<T> extends BaseExpr<T> {
    constructor(private operand: Expression<unknown>, private op: string) {
        super();
    }

    serialize(): Result<string> {
        const [operand, params] = this.operand.serialize();
        return [`(${operand} ${this.op})`, params];
    }
}

class InfixExpr<T> extends BaseExpr<T> {
    constructor(private left: Expression<unknown>, private op: string, private right: Expression<unknown>) {
        super();
    }

    serialize(): Result<string> {
        const [left, params] = this.left.serialize();
        const right = collectParams(this.right.serialize(), params);
        return [`(${left} ${this.op} ${right})`, params];
    }
}

class MultiOperandExpr<T> extends BaseExpr<T> {
    // Like InfixExpr, but can take multiple operands on the right
    constructor(private left: Expression<unknown>, private op: string, private right: Expression<unknown>[]) {
        super();
    }

    serialize(): Result<string> {
        const [left, params] = this.left.serialize();
        const right = this.right.map(e => collectParams(e.serialize(), params));
        return [`(${left} ${this.op} (${right.join(', ')}))`, params];
    }
}

class FuncExpr<T> extends BaseExpr<T> {
    constructor(private functionName: string, private args: Expression<unknown>[]) {
        super();
    }

    serialize(): Result<string> {
        const fn = quote.identifier(this.functionName);
        const [values, params] = serializeArgs(this.args);
        return [`${fn}(${values.join(',')})`, params];
    }
}

export const func = <T>(name: string, args: Expression<unknown>[]): Expression<T> => new FuncExpr(name, args);

export const not = (arg: Expression<boolean>): Expression<boolean> => new PrefixExpr('not', arg);

/** Aggregates not using WITHIN GROUP. */
export class Aggregate<T> extends BaseExpr<T> {
    constructor(private functionName: string, private args: Expression<unknown>[], private doDistinct?: boolean,
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

    serialize(): Result<string> {
        const fn = quote.identifier(this.functionName);
        const [args, params] = serializeArgs(this.args);
        const filter = collectParams(serializeFilterWhere(this.filter), params);
        const distinct = this.doDistinct ? 'DISTINCT ' : '';
        const orderBy = this.order ? ' ' + collectParams(serializeOrderBy(this.order), params) : '';
        return [`${fn}(${distinct}${args.join(',') || '*'}${orderBy})${filter}`, params];
    }
}

/** Aggregates using WITHIN GROUP */
export class OrderedSetAggregate<T> extends BaseExpr<T> {
    constructor(private functionName: string, private args: Expression<unknown>[], private order: OrderArg[],
                private filter?: Expression<boolean>) {
        super();
    }

    filterWhere(filter: Expression<boolean>): Expression<T> {
        return new OrderedSetAggregate(this.functionName, this.args, this.order, filter);
    }

    serialize(): Result<string> {
        const fn = quote.identifier(this.functionName);
        const [args, params] = serializeArgs(this.args);
        const filter = collectParams(serializeFilterWhere(this.filter), params);
        const orderBy = serializeOrderBy(this.order);
        return [`${fn}(${args.join(', ')}) WITHIN GROUP(${orderBy})${filter}`, params];
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

    serialize(): Result<string> {
        const fn = quote.identifier(this.functionName);
        const [args, params] = serializeArgs(this.args);
        const filter = collectParams(serializeFilterWhere(this.filter), params);
        return [`${fn}(${args.join(', ') || '*'})${filter} OVER ${this.over}`, params];
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

class Cast<T> extends BaseExpr<T> {
    constructor(private expression: Expression<unknown>, private toType: string) {
        super();
    }

    serialize(): Result<string> {
        const [expr, params] = this.expression.serialize();
        return [`CAST(${expr} AS ${this.toType})`, params];
    }
}

class ArrayExpr<T> extends BaseExpr<T[]> {
    constructor(private args: Serializable[]) {
        super();
    }

    serialize(): Result<string> {
        const [values, params] = serializeArgs(this.args);
        return [`ARRAY[${values.join(',')}]`, params];
    }
}

export function array<T>(subquery: Subquery<Record<string, Record<string, T>>>): Expression<T[]>;
export function array<T>(...args: Expression<T>[]): Expression<T[]>;
export function array<T>(...args: [Subquery<T>] | Expression<T>[]): Expression<T[]> {
    return new ArrayExpr(args);
}

export const row = <T extends any[]>(...args: {[I in keyof T]: Expression<T[I]>}): Expression<T> =>
    new FuncExpr('ROW', args);

export const exists = (subquery: Subquery<any>) => new FuncExpr<boolean>('EXISTS', [subquery.scalar()]);



class Field<T> extends BaseExpr<T> {
    constructor(private tableName: string, private name: string) {
        super();
    }

    serialize(): Result<string> {
        return [quote.tableName(this.tableName) + quote.columnName(this.name), []];
    }
}

export const field = <T>(tableName: string, name: string): Expression<T> => new Field(tableName, name);

class ParameterExpr<T> extends BaseExpr<T> {
    constructor(private index: number) {
        super();
    }

    serialize(): Result<string> {
        return ['$' + this.index, []];
    }
}

type Parameters<T> = ((t: T) => unknown[]) & {[K in keyof T]: Expression<T[K]>};

export function $<T>(types: {[K in keyof T]: SQL<T[K]>}): Parameters<T> {
    const keys = Object.keys(types) as (keyof T)[];
    const ret = {} as {[K in keyof T]: Expression<T[K]>};
    keys.forEach((n, i) => ret[n as keyof T] = new ParameterExpr(i));
    const ret2 = (t: T): unknown[] => keys.map(k => t[k]);
    return Object.assign(ret2, ret);
}

function serializeArgs(args: Serializable[]): Result<string[]> {
    const params: unknown[] = [];
    const values = args.map(arg => collectParams(arg.serialize(), params));
    return [values, params];
}

function serializeFilterWhere(expr: Expression<boolean> | undefined): Result<string> {
    if (expr) {
        const [f, params] = expr.serialize();
        return [` FILTER(WHERE ${f})`, params];
    }
    return ['', []];
}

function serializeOrderBy(args: OrderArg[]): Result<string> {
    const [exprs, params] = serializeArgs(args.map(a => a.expr));
    const ret = args.map(({order, nulls}, i) => (
        exprs[i] + (order ? ' ' + serializeOrder(order) : '') + (nulls ? ' NULLS ' + nulls : '')
    )).join(',');
    return [ret, params];

    function serializeOrder(order: Order) {
        return order.key + (order.key === 'USING' ? ' ' + order.op : '');
    }
}
