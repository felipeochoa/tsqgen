import { constant } from '../expression';
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

    // - handle NOT IN operation with values
    // - handle NOT IN operation with subquery
    // - handle ANY operation
    // - handle ALL operation
    // - handle ASC order
    // - handle DESC order
    // - handle USING order
});

// ### Constant Tests
// - should serialize constant values correctly

// ### Identifier Tests
// - should serialize identifiers correctly

// ### PrefixExpr Tests
// - should serialize prefix expressions correctly

// ### PostfixExpr Tests
// - should serialize postfix expressions correctly

// ### InfixExpr Tests
// - should serialize infix expressions correctly

// ### MultiOperandExpr Tests
// - should serialize multi-operand expressions correctly

// ### FuncExpr Tests
// - should serialize function expressions correctly

// ### Aggregate Tests
// - should serialize aggregate functions correctly
// - should handle distinct aggregates
// - should handle order by in aggregates
// - should handle filter in aggregates

// ### JsonObjectAgg Tests
// - should serialize JSON object aggregates correctly
// - should handle absent on null option
// - should handle null on null option
// - should handle unique keys option

// ### JsonArrayAgg Tests
// - should serialize JSON array aggregates correctly
// - should handle order by in JSON array aggregates
// - should handle absent on null option
// - should handle null on null option

// ### OrderedSetAggregate Tests
// - should serialize ordered set aggregates correctly
// - should handle filter in ordered set aggregates

// ### WindowCall Tests
// - should serialize window function calls correctly

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
