// Nominal typing helper
const __brand = Symbol('brand');

export type Uuid = {[__brand]: 'uuid'};
export type Enum<E> = {[__brand]: 'enum'; options: E};
export type Json = {[__brand]: 'json'};
export type Jsonb = {[__brand]: 'jsonb'};
export type Xml = {[__brand]: 'xml'};
export type Range<T> = {[__brand]: 'range'; subtype: T};
export type MultiRange<T> = {[__brand]: 'multirange'; subtype: T};

export function tagUuid(hex: string): Uuid {
    const uuidRegex = /^[0-9a-f]{8}-?[0-9a-f]{4}-?[1-5][0-9a-f]{3}-?[89ab][0-9a-f]{3}-?[0-9a-f]{12}$/i;
    if (!uuidRegex.test(hex)) throw new Error('Invalid UUID: ' + hex);
    return hex as unknown as Uuid;
}

// Embed SQL types into TS
interface NullableType<T> {
    [__brand]?: T | null;
    notNull: () => SqlType<T>;
    name: string;
}

export interface SqlType<T> {
    [__brand]?: T; // Needed to make typescript actually check that Expression types line up
    name: string;
}

const makeType = <T>(name: string): NullableType<T> => ({name, notNull: () => ({name})});

export const text = makeType<string>('text');
export const number = makeType<number>('number');
export const boolean = makeType<boolean>('boolean');
export const bytea = makeType<Buffer>('bytea');
export const timestampWithTimeZone = makeType<Date>('timestamp with time zone');
export const uuid = makeType<Uuid>('uuid');
export const json = makeType<Json>('json');
export const jsonb = makeType<Jsonb>('jsonb');
export const xml = makeType<Xml>('xml');

export const enumType = <E extends string>(name: string, ..._opts: E[]): NullableType<Enum<E>> => makeType(name);
export const arrayType = <T>(child: SqlType<T>): NullableType<T[]> => makeType(child.name + '[]');
export const rangeType = <T>(name: string, _child: SqlType<T>): NullableType<Range<T>> => makeType(name);
export const multiRangeType = <T>(name: string, _child: SqlType<T>): NullableType<MultiRange<T>> => makeType(name);

// Convert TS type to SQL
export type SQL<T>
    = T extends string | number | boolean | Buffer | Date | Uuid | null ? SqlType<T>
    : T extends undefined ? never
    : T extends (infer Child)[] ? SqlType<SQL<Child>[]>
    : T extends object ? SqlType<{[K in keyof T]: SQL<T[K]>}>
    : never;
