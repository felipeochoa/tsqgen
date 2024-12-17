// Nominal typing helper
declare const __brand: unique symbol;

export type Uuid = {[__brand]: 'uuid'};
export type Enum<E> = {[__brand]: 'enum', options: E};
export type Json = {[__brand]: 'json'};
export type Jsonb = {[__brand]: 'jsonb'};
export type Xml = {[__brand]: 'xml'};
export type Range<T> = {[__brand]: 'range', subtype: T};
export type MultiRange<T> = {[__brand]: 'multirange', subtype: T};

// Embed SQL types into TS
interface NullableType<T> {
    [__brand]?: T | null;
    notNull(): Type<T>;
}

interface Type<T> {
    [__brand]?: T; // Needed to make typescript actually check that Expression types line up
}

const makeType = <T>(): NullableType<T> => ({notNull: () => ({})});

export const text = makeType<string>();
export const number = makeType<number>();
export const boolean = makeType<boolean>();
export const bytea = makeType<Buffer>();
export const timestampWithTimeZone = makeType<Date>();
export const uuid = makeType<Uuid>();
export const json = makeType<Json>();
export const jsonb = makeType<Jsonb>();
export const xml = makeType<Xml>();

export const enumType = <E extends string>(..._options: E[]): NullableType<Enum<E>> => makeType();
export const arrayType = <T>(_child: Type<T>): NullableType<T[]> => makeType();
export const rangeType = <T>(_child: Type<T>): NullableType<Range<T>> => makeType();
export const multiRangeType = <T>(_child: Type<T>): NullableType<MultiRange<T>> => makeType();

// Convert TS type to SQL
export type SQL<T>
    =  T extends string | number | boolean | Buffer | Date | Uuid | null ? Type<T>
    : T extends undefined ? never
    : T extends Array<infer Child> ? Type<SQL<Child>[]>
    : T extends object ? Type<{[K in keyof T]: SQL<T[K]>}>
    : never;