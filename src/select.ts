import { constant, field, isExpression, Expression, OrderArg, SubqueryExpr } from './expression';
import * as quote from './quote';
import {
    FrameRef, From, GroupingTree, Nullable, RollupArgs, SelectFrom, Subquery, UnitSubq, WindowFrame, WindowParams,
} from './select-types';
import { collectParams, Result, Serializable } from './serialize';
import { SQL } from './types';

/*
 TODO
 - with queries
 - natural join
 - join USING
 - column aliases in from_item
 - TABLESAMPLE
 */
// https://www.postgresql.org/docs/current/sql-select.html

type BoolExpr = Expression<boolean>;
type UnknownExpr = Expression<unknown>;

function tupleMap<T>(): T {
    return new Proxy<any>({}, {
        get(target, tableName) {
            if (tableName in target) return target[tableName];
            if (typeof tableName !== 'string') return undefined;
            return target[tableName] = subTuple(tableName);
        },
    });
}

function subTuple<T>(tableName: string): T {
    return new Proxy<any>({}, {
        get(target, fieldName) {
            if (fieldName in target) return target[fieldName];
            if (typeof fieldName !== 'string') return undefined;
            return target[fieldName] = field(tableName, fieldName);
        },
    });
}

abstract class BaseFrom<T> implements From<T> {
    private tuples: T = tupleMap();

    join<T2>(other: From<T2>, on: (t: T & T2) => BoolExpr): From<T & T2> {
        return new InnerJoin(this, other, on(tupleMap()), false);
    }

    lateral<T2>(otherFn: (t: T) => From<T2>, on: (t: T & T2) => BoolExpr): From<T & T2> {
        const other = otherFn(this.tuples);
        return new InnerJoin(this, other, on(tupleMap()), true);
    }

    leftJoin<T2>(other: From<T2>, on: (t: T & Nullable<T2>) => BoolExpr): From<T & Nullable<T2>> {
        return new LeftJoin(this, other, on(tupleMap()), false);
    }

    leftJoinLateral<T2>(otherFn: (t: T) => From<T2>, on: (t: T & Nullable<T2>) => BoolExpr):
    From<T & Nullable<T2>> {
        const other = otherFn(this.tuples);
        return new LeftJoin(this, other, on(tupleMap()), true);
    }

    rightJoin<T2>(other: From<T2>, on: (t: Nullable<T> & T2) => BoolExpr): From<Nullable<T> & T2> {
        return new RightJoin(this, other, on(tupleMap()));
    }

    crossJoin<T2>(other: From<T2>): From<T & T2> {
        return new CrossJoin(this, other, false);
    }

    crossJoinLateral<T2>(otherFn: (t: T) => From<T2>): From<T & T2> {
        const other = otherFn(this.tuples);
        return new CrossJoin(this, other, true);
    }

    fullJoin<T2>(other: From<T2>, on: (t: Nullable<T & T2>) => BoolExpr): From<Nullable<T & T2>> {
        return new FullJoin(this, other, on(tupleMap()));
    }

    select<SelectTuple>(proj: (t: T) => SelectTuple): SelectFrom<T, SelectTuple> {
        return SubqueryImpl.make(this, proj(tupleMap()));
    }

    abstract serialize(): Result<string>;
}

function serializeJoin<T1, T2>(type: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL', lateral: boolean,
                               left: From<T1>, right: From<T2>, on: BoolExpr): Result<string> {
    const [leftStr, params] = left.serialize();
    const lateralStr = lateral ? ' LATERAL' : '';
    const rightStr = collectParams(right.serialize(), params);
    const onStr = collectParams(on.serialize(), params);
    return [`(${leftStr} ${type} JOIN${lateralStr} ${rightStr} ON ${onStr})`, params];
}

class InnerJoin<T1, T2> extends BaseFrom<T1 & T2> {
    constructor(private left: From<T1>, private right: From<T2>,
                private on: BoolExpr, private isLateral: boolean) {
        super();
    }

    serialize(): Result<string> {
        return serializeJoin('INNER', this.isLateral, this.left, this.right, this.on);
    }
}

class LeftJoin<T1, T2> extends BaseFrom<T1 & Nullable<T2>> {
    constructor(private left: From<T1>, private right: From<T2>,
                private on: BoolExpr, private isLateral: boolean) {
        super();
    }

    serialize(): Result<string> {
        return serializeJoin('LEFT', this.isLateral, this.left, this.right, this.on);
    }
}

class RightJoin<T1, T2> extends BaseFrom<Nullable<T1> & T2> {
    constructor(private left: From<T1>, private right: From<T2>, private on: BoolExpr) {
        super();
    }

    serialize(): Result<string> {
        return serializeJoin('RIGHT', false, this.left, this.right, this.on);
    }
}

class FullJoin<T1, T2> extends BaseFrom<Nullable<T1 & T2>> {
    constructor(private left: From<T1>, private right: From<T2>, private on: BoolExpr) {
        super();
    }

    serialize(): Result<string> {
        return serializeJoin('FULL', false, this.left, this.right, this.on);
    }
}

class CrossJoin<T1, T2> extends BaseFrom<T1 & T2> {
    constructor(private left: From<T1>, private right: From<T2>, private isLateral: boolean) {
        super();
    }

    serialize(): Result<string> {
        const [leftStr, params] = this.left.serialize();
        const rightStr = collectParams(this.right.serialize(), params);
        if (this.isLateral) {
            return [`(${leftStr} CROSS JOIN LATERAL ${rightStr})`, params];
        }
        return [`(${leftStr} CROSS JOIN ${rightStr})`, params];
    }
}

type Tuple<RowType> = {[Field in keyof RowType]: Expression<RowType[Field]>};

// We want to ensure that aliases are only set once per from_item. To do so, we use a "smart constructor" that
// hides the alias parameter, which can only be set from the `.as()` method. In the .as() method we upcast the
// result to hide that method from typescript

class Table<Alias extends string, RowType> extends BaseFrom<Record<Alias, Tuple<RowType>>> {
    protected constructor(private alias: Alias, private realName: string) {
        super();
    }

    /** Create a new table. The second argument exists solely to allow easy type inference/annotation. */
    static define<Name extends string, RowType extends object>(
        name: Name,
        _: {[K in keyof RowType]: SQL<RowType[K]>},
    ) {
        return new Table<Name, RowType>(name, name);
    }

    as<NewAlias extends string>(alias: NewAlias): From<Record<NewAlias, Tuple<RowType>>> {
        return new Table(alias, this.realName);
    }

    serialize(): Result<string> {
        if (this.realName === this.alias) return [quote.tableName(this.alias), []];
        return [`${this.realName} AS ${this.alias}`, []];
    }
}

export const table = Table.define;

class FromFunction<Alias extends string, T> extends BaseFrom<Record<Alias, T>> {
    protected constructor(private alias: Alias, private args: UnknownExpr[],
                          private ordinality: boolean, private realName?: string) {
        super();
    }

    static make<Name extends string, T>(name: Name, args: UnknownExpr[]) {
        return new FromFunction<Name, T>(name, args, false);
    }

    withOrdinality() {
        const ret = new FromFunction<Alias, T>(this.alias, this.args, true, this.realName);
        return ret as Omit<typeof ret, 'withOrdinality'>;
    }

    as<NewAlias extends string>(alias: NewAlias): From<Record<NewAlias, T>> {
        return new FromFunction(alias, this.args, this.ordinality, this.alias);
    }

    serialize(): Result<string> {
        const params: unknown[] = [];
        const args = this.args.map(a => collectParams(a.serialize(), params)).join(', ');
        const withOrdinality = this.ordinality ? ' WITH ORDINALITY' : '';
        const name = quote.identifier(this.realName ?? this.alias);
        const alias = this.realName === undefined ? '' : ' AS ' + quote.identifier(this.alias);
        return [`${name}(${args})${withOrdinality}${alias}`, params];
    }
}

export const fromFunction = FromFunction.make;

class FromSubquery<Alias extends string, T> extends BaseFrom<Record<Alias, T>> {
    constructor(private alias: Alias, private subquery: Subquery<T>) {
        super();
    }

    serialize(): Result<string> {
        const [subquery, params] = this.subquery.serialize();
        const alias = quote.identifier(this.alias);
        return [`(${subquery}) AS ${alias}`, params];
    }
}

// TODO: class RowsFrom<T> extends BaseFrom<T>

export const rollup = (args: RollupArgs): GroupingTree => ({type: 'ROLLUP', args});
export const cube = (args: RollupArgs): GroupingTree => ({type: 'CUBE' as const, args});
export const groupingSets = (args: GroupingTree[]): GroupingTree => ({type: 'GROUPING SETS', args});

type Distinct = {type: 'row'} | {type: 'on', key: UnknownExpr[]};

type WindowState =
    | {type: 'fresh', name: string, partitionBy: UnknownExpr[], orderBy?: OrderArg[], frame?: WindowFrame}
    | {type: 'ref', name: string, existingWindowName: string, orderBy?: OrderArg[], frame?: WindowFrame};

interface SubqueryState<FromTuple, SelectTuple> {
    distinct?: Distinct;
    where?: BoolExpr;
    groupBy?: GroupingTree;
    groupByDistinct: boolean;
    having?: BoolExpr;
    windows: WindowState[];
    setOps: Array<{
        type: 'union' | 'intersect' | 'except',
        all: boolean,
        query: UnitSubq<FromTuple, SelectTuple, any>,
    }>;
    orderBy?: OrderArg[];
    offset?: Expression<number>;
    fetch?: {fetch: Expression<number>, withTies: boolean};
    limit?: Expression<number> | 'ALL';
    locks: Array<{
        strength: 'UPDATE' | 'NO KEY UPDATE' | 'SHARE' | 'KEY SHARE',
        block?: 'NOWAIT' | 'SKIP LOCKED',
        tables?: string[],
    }>;
}

class SubqueryImpl<FromTuple, SelectTuple> {
    protected constructor(private from: From<FromTuple>, private tuple: SelectTuple,
                          private state: SubqueryState<FromTuple, SelectTuple>) {}

    static make<FromTuple, SelectTuple>(from: From<FromTuple>, tuple: SelectTuple) {
        const initState = {groupByDistinct: false, windows: [], setOps: [], locks: []};
        return new SubqueryImpl<FromTuple, SelectTuple>(from, tuple, initState);
    }

    private update(newState: Partial<SubqueryState<FromTuple, SelectTuple>>): SubqueryImpl<FromTuple, SelectTuple>
        { return new SubqueryImpl(this.from, this.tuple, {...this.state, ...newState}); }

    distinct() { return this.update({distinct: {type: 'row'}}); }
    distinctOn(key: ((t: FromTuple) => UnknownExpr[]))
        { return this.update({distinct: {type: 'on', key: key(tupleMap())}}); }

    where(cond: (t: FromTuple) => BoolExpr) { return this.update({where: cond(tupleMap())}); }

    groupBy(dims: (t: FromTuple) => GroupingTree)
        { return this.update({groupBy: dims(tupleMap()), groupByDistinct: false}); }
    groupByDistinct(dims: (t: FromTuple) => GroupingTree)
        { return this.update({groupBy: dims(tupleMap()), groupByDistinct: true}); }
    rollup(dims: (t: FromTuple) => RollupArgs)
        { return this.update({groupBy: rollup(dims(tupleMap())), groupByDistinct: false}); }
    cube(dims: (t: FromTuple) => RollupArgs)
        { return this.update({groupBy: cube(dims(tupleMap())), groupByDistinct: false}); }
    groupingSets(sets: (t: FromTuple) => UnknownExpr[][])
        { return this.update({groupBy: groupingSets(sets(tupleMap())), groupByDistinct: false}); }

    having(cond: (t: FromTuple) => BoolExpr)
        { return this.update({having: cond(tupleMap())}); }

    window(name: string): WindowMaker<FromTuple, SubqueryImpl<FromTuple, SelectTuple>>
        { return new WindowMaker(name, w => this.update({windows: this.state.windows.concat(w)})); }

    private addSetOp(type: 'union' | 'intersect' | 'except', all: boolean,
                     query: UnitSubq<FromTuple, SelectTuple, any>)
        { return this.update({setOps: this.state.setOps.concat({type, all, query})}); }
    union(other: UnitSubq<FromTuple, SelectTuple, any>)
        { return this.addSetOp('union', false, other); }
    unionAll(other: UnitSubq<FromTuple, SelectTuple, any>)
        { return this.addSetOp('union', true, other); }
    intersect(other: UnitSubq<FromTuple, SelectTuple, any>)
        { return this.addSetOp('intersect', false, other); }
    intersectAll(other: UnitSubq<FromTuple, SelectTuple, any>)
        { return this.addSetOp('intersect', true, other); }
    except(other: UnitSubq<FromTuple, SelectTuple, any>)
        { return this.addSetOp('except', false, other); }
    exceptAll(other: UnitSubq<FromTuple, SelectTuple, any>)
        { return this.addSetOp('except', true, other); }

    orderBy(order: (t: FromTuple) => Array<UnknownExpr | OrderArg>)
        { return this.update({orderBy: resolveOrderArgs(order(tupleMap()))}); }

    offset(offset: number | Expression<number>)
        { return this.update({offset: typeof offset === 'number' ? constant(offset) : offset}); }
    limit(limit: number | Expression<number> | 'ALL')
        { return this.update({limit: typeof limit === 'number' ? constant(limit) : limit}); }
    fetch(fetch: number | Expression<number>, ties?: 'WITH TIES') {
        if (typeof fetch === 'number') fetch = constant(fetch);
        const withTies = ties === 'WITH TIES';
        return this.update({fetch: {fetch, withTies}});
    }

    for(strength: 'UPDATE' | 'NO KEY UPDATE' | 'SHARE' | 'KEY SHARE',
        block?: 'NOWAIT' | 'SKIP LOCKED',
        tables?: string[]) {
        return this.update({locks: this.state.locks.concat({strength, block, tables})});
    }

    as<Alias extends string>(alias: Alias): From<Record<Alias, SelectTuple>>
        { return new FromSubquery(alias, this); }
    scalar(): Expression<SelectTuple[keyof SelectTuple]>
        { return new SubqueryExpr(this); }

    serialize(): Result<string> {
        const {state} = this;
        const params: unknown[] = [];
        const s = (x: Serializable) => collectParams(x.serialize(), params);
        const fields = Object.entries(this.tuple);

        const parts = ['SELECT'];
        if (state.distinct)
            parts.push(...collectParams(serializeDistinct(state.distinct), params));
        if (fields.length === 0) parts.push('*');
        else commaSeparate(parts, fields.map(f => serializeField(f)));

        parts.push('FROM', collectParams(this.from.serialize(), params));

        if (state.where) parts.push('WHERE', s(state.where));
        if (state.groupBy) {
            parts.push(...collectParams(serializeGroupBy(state.groupBy, state.groupByDistinct), params));
        }
        if (state.having) parts.push('HAVING', s(state.having));
        if (state.windows.length > 0) {
            parts.push('WINDOW');
            commaSeparate(parts, state.windows.map(w => collectParams(serializeWindow(w), params)));
        }
        for (const {type, all, query} of state.setOps) {
            parts.push(type.toUpperCase());
            if (all) parts.push('ALL');
            parts.push(s(query));
        }
        if (state.orderBy) {
            parts.push('ORDER BY');
            commaSeparate(parts, state.orderBy.map(arg => collectParams(serializeOrderArg(arg), params)));
        }
        parts.push(...collectParams(this.serializeLimits(), params));
        for (const {strength, block, tables} of this.state.locks) {
            parts.push('FOR', strength);
            if (tables !== undefined) parts.push('OF', ...tables.map(quote.tableName));
            if (block !== undefined) parts.push(block);
        }
        return [parts.join(' '), params];
    }

    private serializeLimits(): Result<string[]> {
        const params: unknown[] = [];
        const ret: string[] = [];
        const {offset, limit, fetch} = this.state;
        if (fetch) {
            if (offset === undefined) throw new Error('Recieved FETCH without OFFSET');
            ret.push('OFFSET', collectParams(offset.serialize(), params), 'ROWS');
            ret.push('FETCH NEXT', collectParams(fetch.fetch.serialize(), params), 'ROWS');
            ret.push(fetch.withTies ? 'WITH TIES' : 'ONLY');
            return [ret, params];
        }
        if (limit) {
            ret.push('LIMIT', limit === 'ALL' ? limit : collectParams(limit.serialize(), params));
        }
        if (offset) {
            ret.push('OFFSET', collectParams(offset.serialize(), params));
        }
        return [ret, params];
    }
}

class WindowMaker<FromTuple, Next> {
    constructor(private name: string, private onFinish: (w: WindowState) => Next) {}

    as(_: WindowParams<FromTuple>): Next;
    as(existingWindowName: string, _: Omit<WindowParams<FromTuple>, 'partitionBy'>): Next;
    as(arg1: string | WindowParams<FromTuple>, arg2?: Omit<WindowParams<FromTuple>, 'partitionBy'>) {
        const {frame, orderBy} = typeof arg1 === 'string' ? arg2! : arg1;
        const tuple = tupleMap<FromTuple>();
        const common = {name: this.name, orderBy: orderBy && resolveOrderArgs(orderBy(tuple)), frame};
        const window = typeof arg1 === 'string'
            ? {type: 'ref' as const, existingWindowName: arg1, ...common}
            : {type: 'fresh' as const, partitionBy: arg1.partitionBy(tuple), ...common};
        return this.onFinish(window);
    }
}

function resolveOrderArgs(args: Array<UnknownExpr | OrderArg>): OrderArg[] {
    return args.map(arg => isExpression(arg) ? {expr: arg} : arg);
}

function serializeDistinct(distinct: Distinct): Result<string[]> {
    const ret: string[] = ['DISTINCT'];
    const params: unknown[] = [];
    switch (distinct.type) {
        case 'row': return [ret, params];
        case 'on': {
            ret.push('ON');
            commaSeparate(ret, distinct.key.map(k => [collectParams(k.serialize(), params)]));
            return [ret, params];
        }
        default: assertNever(distinct, 'Invalid distinct type received ' + (distinct as any)?.type);
    }
}

function serializeGroupBy(groupBy: GroupingTree, distinct: boolean): Result<string[]> {
    const ret = ['GROUP BY'];
    const params: unknown[] = [];
    if (distinct) ret.push('DISTINCT');
    go(groupBy, 0);
    return [ret, params];

    function go(tree: GroupingTree, ixInParent: number) {
        if (ixInParent > 0) ret.push(',');
        if (isExpression(tree)) return ret.push(collectParams(tree.serialize(), params));
        if (Array.isArray(tree)) return tree.forEach(go);
        ret.push(tree.type, '(');
        tree.args.forEach(go);
        ret.push(')');
    }
}

function serializeWindow(window: WindowState): Result<string[]> {
    const ret: string[] = [window.name, 'AS', '('];
    const params: unknown[] = [];
    if (window.type === 'fresh') {
        ret.push('PARTITION BY');
        window.partitionBy.forEach((e, i) => {
            i > 0 && ret.push(', ');
            ret.push(collectParams(e.serialize(), params));
        });
    } else {
        ret.push(window.existingWindowName);
    }
    if (window.orderBy)
        window.orderBy.forEach(a => ret.push(...collectParams(serializeOrderArg(a), params)));
    if (window.frame) {
        ret.push(window.frame.type);
        if (window.frame.end !== undefined) ret.push('BETWEEN');
        serializeFrameRef(window.frame.start);
        if (window.frame.end !== undefined) {
            ret.push('AND');
            serializeFrameRef(window.frame.end);
        }
        if (window.frame.exclusion) ret.push(window.frame.exclusion);
    }
    ret.push(')');
    return [ret, params];

    function serializeFrameRef(ref: FrameRef) {
        if (ref.type === 'PRECEDING' || ref.type === 'FOLLOWING')
            ret.push(ref.offset.toString());
        ret.push(ref.type);
    }
}

function serializeOrderArg(arg: OrderArg): Result<string[]> {
    const [expr, params] = arg.expr.serialize();
    const ret = [expr];
    if (arg.order?.key === 'USING') ret.push('USING', arg.order.op);
    else if (arg.order) ret.push(arg.order.key);
    if (arg.nulls) ret.push(arg.nulls);
    return [ret, params];
}

function commaSeparate(into: string[], args: readonly string[][]) {
    if (args.length === 0) return;
    into.push(...args[0]);
    for (let i = 1; i < args.length; i++) {
        into.push(',', ...args[i]);
    }
}

function assertNever(_: never, msg: string): never { // Useful for Typescript exhaustiveness checks
    throw new Error(msg);
}
