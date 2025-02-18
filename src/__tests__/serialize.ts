import {
    Token, columnReference, commaSeparate, identifier, keyWord, literal, operator, specialCharacter, unlex,
} from '../serialize';

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

    test('ColumnReference Tokens', () => {
        const tokens: Token[] = [columnReference('users', 'id')];
        expect(unlex(tokens)).toBe('users.id');
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

    test('dangerous identifier tokens are escaped', () => {
        const dangerousIdentifier = 'DROP "TABLE" users;--';
        const token = identifier(dangerousIdentifier);
        expect(unlex([token])).toBe('"DROP ""TABLE"" users;--"');
    });
});

describe('commaSeparate', () => {
    test('Single Token Array', () => {
        const tokens: Token[][] = [[identifier('column1')]];
        expect(commaSeparate(tokens)).toEqual([identifier('column1')]);
    });

    test('Multiple Token Arrays', () => {
        const tokens: Token[][] = [
            [identifier('column1')],
            [identifier('column2')],
            [identifier('column3')],
        ];
        expect(commaSeparate(tokens)).toEqual([
            identifier('column1'),
            {type: 'SpecialCharacter', value: ','},
            identifier('column2'),
            {type: 'SpecialCharacter', value: ','},
            identifier('column3'),
        ]);
    });

    test('Empty Array', () => {
        const tokens: Token[][] = [];
        expect(commaSeparate(tokens)).toEqual([]);
    });
});

describe('Additional Serialization Tests', () => {
    test('Reserved keyword identifiers are quoted', () => {
        const token = identifier('SELECT');
        expect(unlex([token])).toBe('"SELECT"');
    });

    test('Force-quoted identifiers are always quoted', () => {
        const token = identifier('normal_name', true);
        expect(unlex([token])).toBe('"normal_name"');
    });

    test('Adjacent special characters have correct spacing', () => {
        const tokens: Token[] = [
            specialCharacter('('),
            identifier('col'),
            specialCharacter(')'),
        ];
        expect(unlex(tokens)).toBe('(col)');
    });

    test('Complex mixed token sequences have correct spacing', () => {
        const tokens: Token[] = [
            keyWord('SELECT'),
            identifier('col1'),
            specialCharacter(','),
            identifier('col2'),
            keyWord('FROM'),
            identifier('table'),
            keyWord('WHERE'),
            identifier('id'),
            operator('='),
            literal(42),
        ];
        expect(unlex(tokens)).toBe('SELECT col1, col2 FROM table WHERE id = 42');
    });

    test('Literal strings with quotes are properly escaped', () => {
        const token = literal("O'Reilly");
        expect(unlex([token])).toBe("'O''Reilly'");
    });

    test('Non-finite numbers are properly handled', () => {
        expect(unlex([literal(Infinity)])).toBe("'Infinity'");
        expect(unlex([literal(-Infinity)])).toBe("'-Infinity'");
        expect(unlex([literal(NaN)])).toBe("'NaN'");
    });

    test('Column references are properly formatted', () => {
        const token = columnReference('users', 'email');
        expect(unlex([token])).toBe('users.email');

        // With reserved keywords or special characters
        const token2 = columnReference('user group', 'select');
        expect(unlex([token2])).toBe('"user group"."select"');
    });
});
