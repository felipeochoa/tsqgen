import { Token, commaSeparate, unlex, keyWord, identifier, literal, operator, specialCharacter } from '../serialize';

describe('unlex', () => {
    test('KeyWord Tokens', () => {
        const tokens: Token[] = [keyWord('SELECT')];
        expect(unlex(tokens)).toBe('SELECT');
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
