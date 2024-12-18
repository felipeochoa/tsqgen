import { constant, field, isExpression, Expression, OrderArg, SubqueryExpr } from './expression';
import * as quote from './quote';
import {
    FrameRef, From, GroupingTree, Nullable, RollupArgs, SelectFrom, Subquery, Tuple, TupleMap, UnitSubq,
    WindowFrame, WindowParams,
} from './select-types';
import { commaSeparate, keyWord, literal, identifier, specialCharacter, Token } from './serialize';
import { SQL } from './types';

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
    private tuples: TupleMap<T> = tupleMap();

    join<T2>(other: From<T2>, on: (t: TupleMap<T & T2>) => BoolExpr): From<T & T2> {
        return new InnerJoin(this, other, on(tupleMap()), false);
    }

    lateral<T2>(otherFn: (t: TupleMap<T>) => From<T2>, on: (t: TupleMap<T & T2>) => BoolExpr): From<T & T2> {
        const other = otherFn(this.tuples);
        return new InnerJoin(this, other, on(tupleMap()), true);
    }

    leftJoin<T2>(other: From<T2>, on: (t: TupleMap<T & Nullable<T2>>) => BoolExpr): From<T & Nullable<T2>> {
        return new LeftJoin(this, other, on(tupleMap()), false);
    }

    leftJoinLateral<T2>(otherFn: (t: TupleMap<T>) => From<T2>, on: (t: TupleMap<T & Nullable<T2>>) => BoolExpr):
    From<T & Nullable<T2>> {
        const other = otherFn(this.tuples);
        return new LeftJoin(this, other, on(tupleMap()), true);
    }

    rightJoin<T2>(other: From<T2>, on: (t: TupleMap<Nullable<T> & T2>) => BoolExpr): From<Nullable<T> & T2> {
        return new RightJoin(this, other, on(tupleMap()));
    }

    crossJoin<T2>(other: From<T2>): From<T & T2> {
        return new CrossJoin(this, other, false);
    }

    crossJoinLateral<T2>(otherFn: (t: TupleMap<T>) => From<T2>): From<T & T2> {
        const other = otherFn(this.tuples);
        return new CrossJoin(this, other, true);
    }

    fullJoin<T2>(other: From<T2>, on: (t: TupleMap<Nullable<T & T2>>) => BoolExpr): From<Nullable<T & T2>> {
        return new FullJoin(this, other, on(tupleMap()));
    }

    select<SelectTuple>(proj: (t: TupleMap<T>) => Tuple<SelectTuple>): SelectFrom<T, SelectTuple> {
        return SubqueryImpl.make(this, proj(tupleMap()));
    }

    abstract serialize(): Token[];
}

function serializeJoin<T1, T2>(type: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL', lateral: boolean,
                               left: From<T1>, right: From<T2>, on: BoolExpr): Token[] {
    const leftTokens = left.serialize();
    const lateralTokens = lateral ? [keyWord('LATERAL')] : [];
    const rightTokens = right.serialize();
    const onTokens = on.serialize();
    return [
        specialCharacter('('),
        ...leftTokens,
        keyWord(type),
        keyWord('JOIN'),
        ...lateralTokens,
        ...rightTokens,
        keyWord('ON'),
        ...onTokens,
        specialCharacter(')')
    ];
}

class InnerJoin<T1, T2> extends BaseFrom<T1 & T2> {
    constructor(private left: From<T1>, private right: From<T2>,
                private on: BoolExpr, private isLateral: boolean) {
        super();
    }

    serialize(): Token[] {
        return serializeJoin('INNER', this.isLateral, this.left, this.right, this.on);
    }
}

class LeftJoin<T1, T2> extends BaseFrom<T1 & Nullable<T2>> {
    constructor(private left: From<T1>, private right: From<T2>,
                private on: BoolExpr, private isLateral: boolean) {
        super();
    }

    serialize(): Token[] {
        return serializeJoin('LEFT', this.isLateral, this.left, this.right, this.on);
    }
}

class RightJoin<T1, T2> extends BaseFrom<Nullable<T1> & T2> {
    constructor(private left: From<T1>, private right: From<T2>, private on: BoolExpr) {
        super();
    }

    serialize(): Token[] {
        return serializeJoin('RIGHT', false, this.left, this.right, this.on);
    }
}

class FullJoin<T1, T2> extends BaseFrom<Nullable<T1 & T2>> {
    constructor(private left: From<T1>, private right: From<T2>, private on: BoolExpr) {
        super();
    }

    serialize(): Token[] {
        return serializeJoin('FULL', false, this.left, this.right, this.on);
    }
}

class CrossJoin<T1, T2> extends BaseFrom<T1 & T2> {
    constructor(private left: From<T1>, private right: From<T2>, private isLateral: boolean) {
        super();
    }

    serialize(): Token[] {
        const left = this.left.serialize();
        const rightTokens = this.right.serialize();
        const lateral = this.isLateral ? [keyWord('LATERAL')] : [];
        return [
            specialCharacter('('),
            ...left,
            keyWord('CROSS JOIN'),
            ...lateral,
            ...rightTokens,
            specialCharacter(')')
        ];
    }
}

// We want to ensure that aliases are only set once per from_item. To do so, we use a "smart constructor" that
// hides the alias parameter, which can only be set from the `.as()` method. In the .as() method we upcast the
// result to hide that method from typescript

class Table<Alias extends string, RowType> extends BaseFrom<Record<Alias, RowType>> {
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

    as<NewAlias extends string>(alias: NewAlias): From<Record<NewAlias, RowType>> {
        return new Table(alias, this.realName);
    }

    serialize(): Token[] {
        if (this.realName === this.alias) return [identifier(quote.tableName(this.alias))];
        return [
            identifier(this.realName),
            keyWord('AS'),
            identifier(this.alias)
        ];
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

    serialize(): Token[] {
        const args = commaSeparate(this.args.map(a => a.serialize()));
        const withOrdinality = this.ordinality ? [keyWord('WITH ORDINALITY')] : [];
        const name = identifier(quote.identifier(this.realName ?? this.alias));
        const alias = this.realName === undefined ? [] : [keyWord('AS'), identifier(quote.identifier(this.alias))];
        return [
            name,
            specialCharacter('('),
            ...args,
            specialCharacter(')'),
            ...withOrdinality,
            ...alias
        ];
    }
}

export const fromFunction = FromFunction.make;

class FromSubquery<Alias extends string, T> extends BaseFrom<Record<Alias, T>> {
    constructor(private alias: Alias, private subquery: Subquery<T>) {
        super();
    }

    serialize(): Token[] {
        const subquery = this.subquery.serialize();
        const alias = identifier(quote.identifier(this.alias));
        return [
            specialCharacter('('),
            ...subquery,
            specialCharacter(')'),
            keyWord('AS'),
            alias
        ];
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
        type: 'UNION' | 'INTERSECT' | 'EXCEPT',
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
    protected constructor(private from: From<FromTuple>, private tuple: Tuple<SelectTuple>,
                          private state: SubqueryState<FromTuple, SelectTuple>) {}

    static make<FromTuple, SelectTuple>(from: From<FromTuple>, tuple: Tuple<SelectTuple>) {
        const initState = {groupByDistinct: false, windows: [], setOps: [], locks: []};
        return new SubqueryImpl<FromTuple, SelectTuple>(from, tuple, initState);
    }

    private update(newState: Partial<SubqueryState<FromTuple, SelectTuple>>): SubqueryImpl<FromTuple, SelectTuple>
        { return new SubqueryImpl(this.from, this.tuple, {...this.state, ...newState}); }

    distinct() { return this.update({distinct: {type: 'row'}}); }
    distinctOn(key: ((t: TupleMap<FromTuple>) => UnknownExpr[]))
        { return this.update({distinct: {type: 'on', key: key(tupleMap())}}); }

    where(cond: (t: TupleMap<FromTuple>) => BoolExpr) { return this.update({where: cond(tupleMap())}); }

    groupBy(dims: (t: TupleMap<FromTuple>) => GroupingTree)
        { return this.update({groupBy: dims(tupleMap()), groupByDistinct: false}); }
    groupByDistinct(dims: (t: TupleMap<FromTuple>) => GroupingTree)
        { return this.update({groupBy: dims(tupleMap()), groupByDistinct: true}); }
    rollup(dims: (t: TupleMap<FromTuple>) => RollupArgs)
        { return this.update({groupBy: rollup(dims(tupleMap())), groupByDistinct: false}); }
    cube(dims: (t: TupleMap<FromTuple>) => RollupArgs)
        { return this.update({groupBy: cube(dims(tupleMap())), groupByDistinct: false}); }
    groupingSets(sets: (t: TupleMap<FromTuple>) => UnknownExpr[][])
        { return this.update({groupBy: groupingSets(sets(tupleMap())), groupByDistinct: false}); }

    having(cond: (t: TupleMap<FromTuple>) => BoolExpr)
        { return this.update({having: cond(tupleMap())}); }

    window(name: string): WindowMaker<FromTuple, SubqueryImpl<FromTuple, SelectTuple>>
        { return new WindowMaker(name, w => this.update({windows: this.state.windows.concat(w)})); }

    private addSetOp(type: 'UNION' | 'INTERSECT' | 'EXCEPT', all: boolean,
                     query: UnitSubq<FromTuple, SelectTuple, any>)
        { return this.update({setOps: this.state.setOps.concat({type, all, query})}); }
    union(other: UnitSubq<FromTuple, SelectTuple, any>)
        { return this.addSetOp('UNION', false, other); }
    unionAll(other: UnitSubq<FromTuple, SelectTuple, any>)
        { return this.addSetOp('UNION', true, other); }
    intersect(other: UnitSubq<FromTuple, SelectTuple, any>)
        { return this.addSetOp('INTERSECT', false, other); }
    intersectAll(other: UnitSubq<FromTuple, SelectTuple, any>)
        { return this.addSetOp('INTERSECT', true, other); }
    except(other: UnitSubq<FromTuple, SelectTuple, any>)
        { return this.addSetOp('EXCEPT', false, other); }
    exceptAll(other: UnitSubq<FromTuple, SelectTuple, any>)
        { return this.addSetOp('EXCEPT', true, other); }

    orderBy(order: (t: TupleMap<FromTuple>) => Array<UnknownExpr | OrderArg>)
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

    scalar(): Expression<SelectTuple[keyof SelectTuple]> {
        // https://www.postgresql.org/docs/current/sql-expressions.html#SQL-SYNTAX-SCALAR-SUBQUERIES
        if (Object.keys(this.tuple).length !== 1)
            throw new Error('Scalar subqueries must return exactly one column');
        return new SubqueryExpr({
            serialize: this.serialize.bind(this),
            _tuple: this.tuple,
        });
    }

    serialize(): Token[] {
        const {state} = this;
        const fields = Object.entries<Expression<unknown>>(this.tuple);

        const parts: Token[] = [keyWord('SELECT')];
        if (state.distinct)
            parts.push(...serializeDistinct(state.distinct));
        if (fields.length === 0) parts.push(specialCharacter('*'));
        else parts.push(...commaSeparate(fields.map(([name, expr]) => [
            ...expr.serialize(),
            keyWord('AS'),
            identifier(quote.identifier(name))
        ])));

        parts.push(keyWord('FROM'), ...this.from.serialize());

        if (state.where) parts.push(keyWord('WHERE'), ...state.where.serialize());
        if (state.groupBy) parts.push(...serializeGroupBy(state.groupBy, state.groupByDistinct));
        if (state.having) parts.push(keyWord('HAVING'), ...state.having.serialize());
        if (state.windows.length > 0) {
            parts.push(keyWord('WINDOW'));
            parts.push(...commaSeparate(state.windows.map(w => serializeWindow(w))));
        }
        for (const {type, all, query} of state.setOps) {
            parts.push(keyWord(type));
            if (all) parts.push(keyWord('ALL'));
            parts.push(...query.serialize());
        }
        if (state.orderBy) {
            parts.push(keyWord('ORDER BY'));
            parts.push(...commaSeparate(state.orderBy.map(arg => serializeOrderArg(arg))));
        }
        parts.push(...this.serializeLimits());
        for (const {strength, block, tables} of this.state.locks) {
            parts.push(keyWord('FOR'), keyWord(strength));
            if (tables !== undefined) parts.push(keyWord('OF'), ...tables.map(quote.tableName).map(identifier));
            if (block !== undefined) parts.push(keyWord(block));
        }
        return parts;
    }

    private serializeLimits(): Token[] {
        const ret: Token[] = [];
        const {offset, limit, fetch} = this.state;
        if (fetch) {
            if (offset === undefined) throw new Error('Recieved FETCH without OFFSET');
            ret.push(keyWord('OFFSET'), ...offset.serialize(), keyWord('ROWS'));
            ret.push(keyWord('FETCH NEXT'), ...fetch.fetch.serialize(), keyWord('ROWS'));
            ret.push(keyWord(fetch.withTies ? 'WITH TIES' : 'ONLY'));
            return ret;
        }
        if (limit) {
            ret.push(keyWord('LIMIT'), ...(limit === 'ALL' ? [keyWord(limit)] : limit.serialize()));
        }
        if (offset) {
            ret.push(keyWord('OFFSET'), ...offset.serialize());
        }
        return ret;
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

function serializeDistinct(distinct: Distinct): Token[] {
    const ret: Token[] = [keyWord('DISTINCT')];
    switch (distinct.type) {
        case 'row': return ret;
        case 'on': {
            ret.push(keyWord('ON'));
            ret.push(...commaSeparate(distinct.key.map(k => k.serialize())));
            return ret;
        }
        default: assertNever(distinct, 'Invalid distinct type received ' + (distinct as any)?.type);
    }
}

function serializeGroupBy(groupBy: GroupingTree, distinct: boolean): Token[] {
    const ret: Token[] = [keyWord('GROUP BY')];
    if (distinct) ret.push(keyWord('DISTINCT'));
    go(groupBy, 0);
    return ret;

    function go(tree: GroupingTree, ixInParent: number) {
        if (ixInParent > 0) ret.push(specialCharacter(','));
        if (isExpression(tree)) return ret.push(...tree.serialize());
        if (Array.isArray(tree)) return tree.forEach(go);
        ret.push(keyWord(tree.type), specialCharacter('('));
        tree.args.forEach(go);
        ret.push(specialCharacter(')'));
    }
}

function serializeWindow(window: WindowState): Token[] {
    const ret: Token[] = [identifier(window.name), keyWord('AS'), specialCharacter('(')];
    if (window.type === 'fresh') {
        ret.push(keyWord('PARTITION BY'));
        window.partitionBy.forEach((e, i) => {
            i > 0 && ret.push(specialCharacter(','));
            ret.push(...e.serialize());
        });
    } else {
        ret.push(identifier(window.existingWindowName));
    }
    if (window.orderBy)
        window.orderBy.forEach(a => ret.push(...serializeOrderArg(a)));
    if (window.frame) {
        ret.push(keyWord(window.frame.type));
        if (window.frame.end !== undefined) ret.push(keyWord('BETWEEN'));
        serializeFrameRef(window.frame.start);
        if (window.frame.end !== undefined) {
            ret.push(keyWord('AND'));
            serializeFrameRef(window.frame.end);
        }
        if (window.frame.exclusion) ret.push(keyWord(window.frame.exclusion));
    }
    ret.push(specialCharacter(')'));
    return ret;

    function serializeFrameRef(ref: FrameRef) {
        if (ref.type === 'PRECEDING' || ref.type === 'FOLLOWING')
            ret.push(literal(ref.offset.toString()));
        ret.push(keyWord(ref.type));
    }
}

function serializeOrderArg(arg: OrderArg): Token[] {
    const ret: Token[] = [...arg.expr.serialize()];
    if (arg.order?.key === 'USING') ret.push(keyWord('USING'), identifier(arg.order.op));
    else if (arg.order) ret.push(keyWord(arg.order.key));
    if (arg.nulls) ret.push(keyWord(arg.nulls));
    return ret;
}

function assertNever(_: never, msg: string): never { // Useful for Typescript exhaustiveness checks
    throw new Error(msg);
}
