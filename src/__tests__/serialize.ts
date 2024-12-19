import { Token, commaSeparate, unlex, keyWord, identifier, literal, operator, specialCharacter } from '../serialize';

describe('unlex', () => {
    test('KeyWord Tokens', () => {
        const tokens: Token[] = [keyWord('SELECT')];
        expect(unlex(tokens)).toBe('SELECT');
    });

    test('Literal Tokens with Number', () => {
        const tokens: Token[] = [literal(42)];
        expect(unlex(tokens)).toBe('42');
    });

    test('Literal Tokens with Boolean', () => {
        const tokens: Token[] = [literal(true)];
        expect(unlex(tokens)).toBe('true');
    });

    test('Literal Tokens with Null', () => {
        const tokens: Token[] = [literal(null)];
        expect(unlex(tokens)).toBe('null');
    });

    test('Literal Tokens with Non-finite Number', () => {
        const tokens: Token[] = [literal(Infinity)];
        expect(unlex(tokens)).toBe("'Infinity'");
    });

    test('Identifier Tokens', () => {
        const tokens: Token[] = [identifier('my_table')];
        expect(unlex(tokens)).toBe('my_table');
    });

    test('Literal Tokens', () => {
        const tokens: Token[] = [literal('value')];
        expect(unlex(tokens)).toBe("'value'");
    });

    test('Operator Tokens', () => {
        const tokens: Token[] = [operator('+')];
        expect(unlex(tokens)).toBe('+');
    });

    test('SpecialCharacter Tokens', () => {
        const tokens: Token[] = [specialCharacter('(')];
        expect(unlex(tokens)).toBe('(');
    });

    test('Mixed Tokens', () => {
        const tokens: Token[] = [
            keyWord('SELECT'),
            identifier('column'),
            keyWord('FROM'),
            identifier('table'),
        ];
        expect(unlex(tokens)).toBe('SELECT column FROM table');
    });
});

describe('commaSeparate', () => {
    test('Single Token Array', () => {
        const tokens: Token[][] = [[{type: 'Identifier', value: 'column1'}]];
        expect(commaSeparate(tokens)).toEqual([{type: 'Identifier', value: 'column1'}]);
    });

    test('Multiple Token Arrays', () => {
        const tokens: Token[][] = [
            [{type: 'Identifier', value: 'column1'}],
            [{type: 'Identifier', value: 'column2'}],
            [{type: 'Identifier', value: 'column3'}],
        ];
        expect(commaSeparate(tokens)).toEqual([
            {type: 'Identifier', value: 'column1'},
            {type: 'SpecialCharacter', value: ','},
            {type: 'Identifier', value: 'column2'},
            {type: 'SpecialCharacter', value: ','},
            {type: 'Identifier', value: 'column3'},
        ]);
    });

    test('Empty Array', () => {
        const tokens: Token[][] = [];
        expect(commaSeparate(tokens)).toEqual([]);
    });
});
