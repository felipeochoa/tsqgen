import { Expression, OrderArg } from './expression';
import { Result } from './serialize';

type BoolExpr = Expression<boolean>;
type UnknownExpr = Expression<unknown>;

type Nullable1<T> = {[K in keyof T]: T[K] | null};
export type Nullable<T> = {[K in keyof T]: Nullable1<T[K]>};

export interface From<T> {
    join<T2>(other: From<T2>, on: (t: T & T2) => BoolExpr):                                From<T & T2>;
    lateral<T2>(other: (t: T) => From<T2>, on: (t: T & T2) => BoolExpr):                   From<T & T2>;
    leftJoin<T2>(other: From<T2>, on: (t: T & Nullable<T2>) => BoolExpr):                  From<T & Nullable<T2>>;
    leftJoinLateral<T2>(other: (t: T) => From<T2>, on: (t: T & Nullable<T2>) => BoolExpr): From<T & Nullable<T2>>;
    rightJoin<T2>(other: From<T2>, on: (t: Nullable<T> & T2) => BoolExpr):                 From<Nullable<T> & T2>;
    fullJoin<T2>(other: From<T2>, on: (t: Nullable<T & T2>) => BoolExpr):                  From<Nullable<T & T2>>;
    crossJoin<T2>(other: From<T2>):                                                        From<T & T2>;
    crossJoinLateral<T2>(other: (t: T) => From<T2>):                                       From<T & T2>;
    select<SelectTuple>(proj: (t: T) => SelectTuple): SelectFrom<T, SelectTuple>;
    serialize(): Result<string>;
}

export type SelectFrom<FromTuple, SelectTuple> = DistinctSubq<FromTuple, SelectTuple, true> & {
    distinct(): DistinctSubq<FromTuple, SelectTuple, false>;
    distinctOn(key: ((t: FromTuple) => UnknownExpr[])): DistinctSubq<FromTuple, SelectTuple, false>;
};

type DistinctSubq<FromTuple, SelectTuple, AllowLock> = FilteredSubq<FromTuple, SelectTuple, AllowLock> & {
    where(cond: (t: FromTuple) => BoolExpr): FilteredSubq<FromTuple, SelectTuple, AllowLock>;
};

// https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS
export type GroupingTree =
    | UnknownExpr
    | UnknownExpr[]
    | {type: 'ROLLUP' | 'CUBE', args: RollupArgs}
    | {type: 'GROUPING SETS', args: GroupingTree[]};

export type RollupArgs = Array<UnknownExpr | UnknownExpr[]>;

type FilteredSubq<FromTuple, SelectTuple, AllowLock> = GroupedSubq<FromTuple, SelectTuple, AllowLock> & {
    groupBy(dims: (t: FromTuple) => GroupingTree):         GroupedSubq<FromTuple, SelectTuple, false>;
    groupByDistinct(dims: (t: FromTuple) => GroupingTree): GroupedSubq<FromTuple, SelectTuple, false>;
    rollup(dims: (t: FromTuple) => RollupArgs):            GroupedSubq<FromTuple, SelectTuple, false>;
    cube(dims: (t: FromTuple) => RollupArgs):              GroupedSubq<FromTuple, SelectTuple, false>;
    groupingSets(sets: (t: FromTuple) => UnknownExpr[][]): GroupedSubq<FromTuple, SelectTuple, false>;
};

type GroupedSubq<FromTuple, SelectTuple, AllowLock> = GroupedFilteredSubq<FromTuple, SelectTuple, AllowLock> & {
    having(cond: (t: FromTuple) => BoolExpr): GroupedFilteredSubq<FromTuple, SelectTuple, false>;
};

type GroupedFilteredSubq<FromTuple, SelectTuple, AllowLock> = UnitSubq<FromTuple, SelectTuple, AllowLock> & {
    // TODO: Can we make inter-window references type-safe and enforce that `orderBy` only be given once?
    window(name: string): WindowMaker<FromTuple, SelectTuple, AllowLock>;
};

type WindowMaker<FromTuple, SelectTuple, AllowLock> = {
    as(_: WindowParams<FromTuple>): GroupedFilteredSubq<FromTuple, SelectTuple, AllowLock>;
    as(existingWindowName: string, _?: Omit<WindowParams<FromTuple>, 'partitionBy'>):
        GroupedFilteredSubq<FromTuple, SelectTuple, AllowLock>
};

export type WindowParams<FromTuple> = {
    partitionBy: (t: FromTuple) => UnknownExpr[];
    orderBy?: (t: FromTuple) => (UnknownExpr | OrderArg)[];
    frame?: WindowFrame;
};

export interface WindowFrame {
    type: 'RANGE' | 'ROWS' | 'GROUPS';
    start: Exclude<FrameRef, {type: 'UNBOUNDED FOLLOWING'}>;
    end?: Exclude<FrameRef, {type: 'UNBOUNDED PRECEDING'}>;
    exclusion?: 'EXCLUDE CURRENT ROW' | 'EXCLUDE GROUP' | 'EXCLUDE TIES' | 'EXCLUDE NO OTHERS';
}

export type FrameRef =
    | {type: 'UNBOUNDED PRECEDING'}
    | {type: 'CURRENT ROW'}
    | {type: 'UNBOUNDED FOLLOWING'}
    | {offset: number, type: 'PRECEDING' | 'FOLLOWING'};

export type UnitSubq<FromTuple, SelectTuple, AllowLock> = OrderableSubq<FromTuple, SelectTuple, AllowLock> & {
    // TODO: ORDER BY and LIMIT can be attached to a subexpression if it is enclosed in parentheses. Without
    // parentheses, these clauses will be taken to apply to the result of the UNION, not to its right-hand input
    // expression.
    union(other: UnitSubq<FromTuple, SelectTuple, any>):        UnitSubq<FromTuple, SelectTuple, false>;
    unionAll(other: UnitSubq<FromTuple, SelectTuple, any>):     UnitSubq<FromTuple, SelectTuple, false>;
    intersect(other: UnitSubq<FromTuple, SelectTuple, any>):    UnitSubq<FromTuple, SelectTuple, false>;
    intersectAll(other: UnitSubq<FromTuple, SelectTuple, any>): UnitSubq<FromTuple, SelectTuple, false>;
    except(other: UnitSubq<FromTuple, SelectTuple, any>):       UnitSubq<FromTuple, SelectTuple, false>;
    exceptAll(other: UnitSubq<FromTuple, SelectTuple, any>):    UnitSubq<FromTuple, SelectTuple, false>;
};

type OrderableSubq<FromTuple, SelectTuple, AllowLock> = OrderedSubq<SelectTuple, AllowLock> & {
    // A limitation of this feature is that an ORDER BY clause applying to the result of a UNION, INTERSECT, or
    // EXCEPT clause can only specify an output column name or number, not an expression.
    orderBy(order: (t: FromTuple) => (UnknownExpr | OrderArg)[]): OrderedSubq<SelectTuple, AllowLock>;
};

// Postgres uses OFFSET/LIMIT, but also supports SQL:2008's OFFSET/FETCH. With OFFSET/LIMIT, the clauses can be in
// any order, but with SQL:08, OFFSET must come first.

type OrderedSubq<SelectTuple, AllowLock> = OrderedSubq1<SelectTuple, AllowLock>
                                         | OrderedSubq2<SelectTuple, AllowLock>;

type OrderedSubq1<SelectTuple, AllowLock> = LockableSubq<SelectTuple, AllowLock> & {
    offset(start: number | Expression<number>): OffsetSubq<SelectTuple, AllowLock>;
};

type OffsetSubq<SelectTuple, AllowLock> = LockableSubq<SelectTuple, AllowLock> & {
    limit(count: number | Expression<number> | 'ALL'):              LockableSubq<SelectTuple, AllowLock>;
    fetch(count: number | Expression<number>, ties?: 'WITH TIES'): LockableSubq<SelectTuple, AllowLock>;
    // The WITH TIES option is used to return any additional rows that tie for the last place in the result set
    // according to the ORDER BY clause; ORDER BY is mandatory in this case, and SKIP LOCKED is not allowed.
};

type OrderedSubq2<SelectTuple, AllowLock> = LockableSubq<SelectTuple, AllowLock> & {
    limit(count: number | Expression<number>): LimitedSubq<SelectTuple, AllowLock>;
};

type LimitedSubq<SelectTuple, AllowLock> = LockableSubq<SelectTuple, AllowLock> & {
    offset(start: number | Expression<number>): LockableSubq<SelectTuple, AllowLock>;
};

type LockableSubq<SelectTuple, AllowLock> = Subquery<SelectTuple> & (AllowLock extends true ? {
    for(lockStrength: 'UPDATE' | 'NO KEY UPDATE' | 'SHARE' | 'KEY SHARE',
        block?: 'NOWAIT' | 'SKIP LOCKED',
        tables?: string[]): LockableSubq<SelectTuple, true>;
} : {});

export interface Subquery<SelectTuple> {
    as<K extends string>(alias: K): From<Record<K, SelectTuple>>;
    scalar(): Expression<SelectTuple[keyof SelectTuple]>;
    serialize(): Result<string>;
}
