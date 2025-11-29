package edu.stevens.cs562;

/**
 * Represents ONE atomic condition of the form:
 *
 *     left  operator  right
 *
 *
 *  - "negated" handles unary NOT (NOT condition).
 *  - left  and right are stored as raw strings.
 *  - operator is one of: >, >=, <, <=, =, <>
 *
 * ConditionExpression.java handles chaining (AND/OR).
 */
public class Condition {

    public String left;       // left side of condition
    public String operator;   // >, <, >=, <=, =, <>
    public String right;      // right side of the condition
    public boolean negated;   // true if condition was prefixed with NOT

    public Condition(String left, String operator, String right, boolean negated) {
        this.left     = left.trim();
        this.operator = operator.trim();
        this.right    = right.trim();
        this.negated  = negated;
    }

    @Override
    public String toString() {
        String base = left + " " + operator + " " + right;
        return negated ? ("NOT " + base) : base;
    }
}