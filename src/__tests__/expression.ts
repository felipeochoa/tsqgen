import { array, constant, field, func, not } from '../expression';
import { JsonArrayAgg, JsonObjectAgg, PartialWindowCall, agg } from '../expression';
import { Serializable, unlex } from '../serialize';
import { boolean } from '../types';

describe('BaseExpr', () => {
    it('.isNull()', () => {
        const expr = constant('abc').isNull();
        expectStringifyToBe(expr, "('abc' IS NULL)");
    });

    it('.isNotNull()', () => {
        const expr = constant('abc').isNotNull();
        expectStringifyToBe(expr, "('abc' IS NOT NULL)");
    });

    it('.or()', () => {
        const expr1 = constant(true);
        const expr2 = constant(false);
        expectStringifyToBe(expr1.or(expr2), "(true OR false)");
    });

    it('.and()', () => {
        const expr1 = constant(true);
        const expr2 = constant(false);
        expectStringifyToBe(expr1.and(expr2), "(true AND false)");
    });

    it('.isDistinctFrom()', () => {
        const expr1 = constant(1);
        const expr2 = constant(2);
        expectStringifyToBe(expr1.isDistinctFrom(expr2), "(1 IS DISTINCT FROM 2)");
    });

    it('.isNotDistinctFrom()', () => {
        const expr1 = constant(1);
        const expr2 = constant(2);
        expectStringifyToBe(expr1.isNotDistinctFrom(expr2), "(1 IS NOT DISTINCT FROM 2)");
    });

    it('.eq()', () => {
        const expr1 = constant(1);
        const expr2 = constant(1);
        expectStringifyToBe(expr1.eq(expr2), "(1 = 1)");
    });

    it('.ne()', () => {
        const expr1 = constant(1);
        const expr2 = constant(2);
        expectStringifyToBe(expr1.ne(expr2), "(1 <> 2)");
    });

    it('.lt()', () => {
        const expr1 = constant(1);
        const expr2 = constant(2);
        expectStringifyToBe(expr1.lt(expr2), "(1 < 2)");
    });

    it('.le()', () => {
        const expr1 = constant(1);
        const expr2 = constant(2);
        expectStringifyToBe(expr1.le(expr2), "(1 <= 2)");
    });

    it('.gt()', () => {
        const expr1 = constant(2);
        const expr2 = constant(1);
        expectStringifyToBe(expr1.gt(expr2), "(2 > 1)");
    });

    it('.ge()', () => {
        const expr1 = constant(2);
        const expr2 = constant(1);
        expectStringifyToBe(expr1.ge(expr2), "(2 >= 1)");
    });

    it('.like()', () => {
        const expr1 = constant('abc');
        const expr2 = constant('%b%');
        expectStringifyToBe(expr1.like(expr2), "('abc' LIKE '%b%')");
    });

    it('.ilike()', () => {
        const expr1 = constant('abc');
        const expr2 = constant('%B%');
        expectStringifyToBe(expr1.ilike(expr2), "('abc' ILIKE '%B%')");
    });

    it('.collate()', () => {
        const expr = constant('abc').collate('en_US');
        expectStringifyToBe(expr, `('abc' COLLATE "en_US")`);
    });

    it('.castAs()', () => {
        const expr = constant(1).castAs(boolean);
        expectStringifyToBe(expr, "CAST(1 AS boolean)");
    });

    it('.in() with values', () => {
        const expr = constant(1).in(constant(1), constant(2), constant(3));
        expectStringifyToBe(expr, "(1 IN (1, 2, 3))");
    });

    it.todo('.in() with subquery');

    it('.notIn() with values', () => {
        const expr = constant(1).notIn(constant(1), constant(2), constant(3));
        expectStringifyToBe(expr, "(1 NOT IN (1, 2, 3))");
    });

    it.todo('.notIn() with subquery');

    it('.any()', () => {
        const expr = constant(1).any('=', array(constant(1), constant(2), constant(3)));
        expectStringifyToBe(expr, "(1 = ANY(ARRAY[1, 2, 3]))");
    });

    it.todo('.any() with subquery');

    it('.all()', () => {
        const expr = constant(1).all('<>', array(constant(1), constant(2), constant(3)));
        expectStringifyToBe(expr, "(1 <> ALL(ARRAY[1, 2, 3]))");
    });

    it.todo('.all() with subquery');

    it.todo('.asc()');

    it.todo('.desc()');

    it.todo('.using()');
});

it('serializes constant values correctly', () => {
    expectStringifyToBe(constant('hello'), "'hello'");
    expectStringifyToBe(constant(123), "123");
    expectStringifyToBe(constant(false), "false");
});

it('serializes identifier expressions correctly', () => {
    const expr = field('users', 'name');
    expectStringifyToBe(expr, "users.name");
});

it('serializes prefix expressions correctly', () => {
    const expr = not(constant(true));
    expectStringifyToBe(expr, "(not true)");
});

it('serializes postfix expressions correctly', () => {
    const expr = constant('postfix').isNotNull();
    expectStringifyToBe(expr, "('postfix' IS NOT NULL)");
});

it('serializes infix expressions correctly', () => {
    const expr = constant(1).eq(constant(1));
    expectStringifyToBe(expr, "(1 = 1)");
});

it('serializes multi-operand expressions correctly', () => {
    const expr = constant(1).in(constant(1), constant(2), constant(3));
    expectStringifyToBe(expr, "(1 IN (1, 2, 3))");
});

it('serializes function expressions correctly', () => {
    const expr = func('MY_FUNC', [constant(1), constant('two')]);
    expectStringifyToBe(expr, "MY_FUNC(1, 'two')");
});

it('serializes function expressions correctly with no arguments', () => {
    const expr = func('MY_FUNC', []);
    expectStringifyToBe(expr, "MY_FUNC()");
});

it('serializes function expressions correctly with one argument', () => {
    const expr = func('MY_FUNC', [constant(1)]);
    expectStringifyToBe(expr, "MY_FUNC(1)");
});

it('serializes aggregate functions correctly', () => {
    const expr = agg<number>('SUM', [field('users', 'age')]);
    expectStringifyToBe(expr, "SUM(users.age)");
});

it('handles distinct aggregates', () => {
    const expr = agg<number>('SUM', [field('users', 'income')]).distinct();
    expectStringifyToBe(expr, "SUM(DISTINCT users.income)");
});

it('handles order by in aggregates (ordered-set aggregate)', () => {
    const expr = agg<number>('PERCENTILE_CONT', [field('users', 'score')], 'WITHIN GROUP', [field('users', 'score').asc()]);
    expectStringifyToBe(expr, "PERCENTILE_CONT(users.score) WITHIN GROUP (users.score ASC)");
});

it('handles filter in aggregates', () => {
    const expr = agg<number>('COUNT', [field('users', 'id')])
        .filterWhere(field<boolean>('users', 'active').eq(constant(true)));
    expectStringifyToBe(expr, "COUNT(users.id) FILTER (WHERE (users.active = true))");
});

it('serializes JSON object aggregates correctly', () => {
    const expr = new JsonObjectAgg(field('users', 'name'), field('users', 'id'));
    expectStringifyToBe(expr, "json_object_agg(users.name: users.id)");
});

it('handles absent on null option for JSON object aggregates', () => {
    const expr = new JsonObjectAgg(field('users', 'name'), field('users', 'id')).absentOnNull();
    expectStringifyToBe(expr, "json_object_agg(users.name: users.id ABSENT ON NULL)");
});

it('handles null on null option for JSON object aggregates', () => {
    const expr = new JsonObjectAgg(field('users', 'name'), field('users', 'id')).nullOnNull();
    expectStringifyToBe(expr, "json_object_agg(users.name: users.id NULL ON NULL)");
});

it('handles unique keys option for JSON object aggregates', () => {
    const expr = new JsonObjectAgg(field('users', 'name'), field('users', 'id')).withUniqueKeys();
    expectStringifyToBe(expr, "json_object_agg(users.name: users.id WITH UNIQUE KEYS)");
});

it('serializes JSON array aggregates correctly', () => {
    const expr = new JsonArrayAgg(field('users', 'id'));
    expectStringifyToBe(expr, "json_array_agg(users.id)");
});

it('handles order by in JSON array aggregates', () => {
    const expr = new JsonArrayAgg(field('users', 'id')).orderBy([field('users', 'name').desc()]);
    expectStringifyToBe(expr, "json_array_agg(users.id ORDER BY users.name DESC)");
});

it('handles absent on null option for JSON array aggregates', () => {
    const expr = new JsonArrayAgg(field('users', 'id')).absentOnNull();
    expectStringifyToBe(expr, "json_array_agg(users.id ABSENT ON NULL)");
});

it('handles null on null option for JSON array aggregates', () => {
    const expr = new JsonArrayAgg(field('users', 'id')).nullOnNull();
    expectStringifyToBe(expr, "json_array_agg(users.id NULL ON NULL)");
});

it('handles filter in ordered set aggregates', () => {
    const expr = agg('PERCENTILE_CONT', [field('users', 'score')], 'WITHIN GROUP', [field('users', 'score').asc()])
        .filterWhere(field<boolean>('users', 'active').eq(constant(true)));
    expectStringifyToBe(expr,
                        "PERCENTILE_CONT(users.score) WITHIN GROUP (users.score ASC) FILTER (WHERE (users.active = true))");
});

it('serializes window function calls correctly', () => {
    const partial = new PartialWindowCall<number>('ROW_NUMBER', []);
    const expr = partial.over('win_alias');
    expectStringifyToBe(expr, "ROW_NUMBER(*) OVER win_alias");
});

// ### PartialWindowCall Tests
// - should handle filter in partial window calls
// - should handle over clause in partial window calls

// ### Cast Tests
// - should serialize cast expressions correctly

// ### ArrayExpr Tests
// - should serialize array expressions correctly

// ### Field Tests
// - should serialize field expressions correctly

// ### ParameterExpr Tests
// - should serialize parameter expressions correctly

// ### Utility Function Tests
// - should correctly identify expressions using isExpression
// - should correctly identify final expressions using isFinalExpression
// - should correctly create constant expressions
// - should correctly create function expressions
// - should correctly create NOT expressions
// - should correctly create array expressions
// - should correctly create row expressions
// - should correctly create EXISTS expressions
// - should correctly create field expressions
// - should correctly create parameter expressions

const stringify = (s: Serializable): string => unlex(s.serialize());

const expectStringifyToBe = (expr: Serializable, expected: string) => expect(stringify(expr)).toBe(expected);
