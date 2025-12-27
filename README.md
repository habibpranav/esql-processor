# ESQL Processor

Query processing engine for ad-hoc OLAP queries using the Extended Multi-Feature (EMF) query model and Phi operator.

CS 562 - Database Management Systems II
Stevens Institute of Technology, Fall 2025


## Problem

Ad-hoc OLAP queries in standard SQL require multiple joins, group-bys, and subqueries. Traditional optimizers don't see the big picture - they optimize individual operations, leading to poor performance.

Example: Find average sales per customer in NY, NJ, and CT states.
Standard SQL needs 3 subqueries joined together. EMF does it in one query with zero joins.


## Solution

EMF (Extended Multi-Feature) query syntax extends GROUP BY with grouping variables (x, y, z...), adds SUCH THAT clause to define predicates for each grouping variable, uses Phi operator to represent the query algebraically, and evaluates using mf-structure (in-memory) with multiple table scans instead of joins.


## Phi Operator

Phi(S, n, V, F, sigma, G)

S     - Projected attributes for output
n     - Number of grouping variables
V     - Grouping attributes (e.g., cust, prod)
F     - Aggregate functions (sum, avg, count, min, max)
sigma - Predicates: sigma0=WHERE, sigma1..n=SUCH THAT conditions
G     - HAVING clause


## EMF Evaluation Algorithm

mf_struct:
    - grouping attributes (from V)
    - aggregate values for each grouping variable (from F)
    - supporting fields for computing aggregates (count, sum for avg)

H = array of mf_struct entries

main():

    Scan 0 - Build mf-structure with distinct grouping attribute values:
        for each tuple t in table:
            if t satisfies sigma0 (WHERE):
                if no entry in H matches t's grouping attributes:
                    create new entry with t's grouping attributes
                    initialize all aggregate fields

    Scans 1 to n - Compute aggregates for each grouping variable:
        for i = 1 to n:
            for each tuple t in table:
                if t satisfies sigma0 (WHERE):
                    for each entry e in H:
                        if t satisfies sigma[i] with respect to e:
                            update e's aggregates for grouping variable i

    Output - Apply HAVING and project:
        for each entry e in H:
            if e satisfies G (HAVING):
                output projected attributes S from e

Key difference from MF: In EMF, grouping variables can range over the entire
relation, not just the current group. A single tuple t may update multiple
entries in H during each scan. This is why we check all entries of H for
each tuple.

Key point: Only table scans. No joins. No subqueries. mf-structure holds
everything in memory.



## Schema

sales(cust, prod, day, month, year, state, quant, date)


## Example Query

Query: For each customer, find average quantity in NY, CT, NJ where NY average exceeds both others.

ESQL Syntax:

    SELECT cust, avg(X.quant), avg(Y.quant), avg(Z.quant)
    FROM sales
    GROUP BY cust; X, Y, Z
    SUCH THAT
        X.cust = cust AND X.state = 'NY',
        Y.cust = cust AND Y.state = 'CT',
        Z.cust = cust AND Z.state = 'NJ'
    HAVING avg(X.quant) > avg(Y.quant) AND avg(X.quant) > avg(Z.quant)

Phi Representation:

    S:     cust, avg(X.quant), avg(Y.quant), avg(Z.quant)
    n:     3
    V:     cust
    F:     avg_1_quant, avg_2_quant, avg_3_quant
    sigma: sigma0 = (none)
           sigma1 = X.state = 'NY'
           sigma2 = Y.state = 'CT'
           sigma3 = Z.state = 'NJ'
    G:     avg_1_quant > avg_2_quant AND avg_1_quant > avg_3_quant


## Technology Stack

Language: Java 21
Build: Maven
Database: PostgreSQL
JDBC: PostgreSQL JDBC 42.7.7
IDE: IntelliJ IDEA


## Key Features

- Interactive Phi Operator Mode for step-by-step guided input
- Automatic code generation tailored to the query and schema
- Dynamic schema support via JDBC metadata
- Supports both ESQL syntax and Phi operator format
- Accepts string input, file input, or interactive prompts


## Limitations

- String-based parsing using regex instead of a proper lexer/tokenizer
- Limited input validation so invalid queries may be accepted
- No syntax error recovery so parser may fail silently
- Some parsing is sensitive to whitespace or casing variations


## Design Decision: String Parsing vs ANTLR

The choice was string manipulation and regex vs proper lexer/tokenizer (ANTLR).

Why I chose strings: Time pressure with deadline approaching. Seemed simpler and faster. Didn't anticipate edge cases. Thought learning ANTLR would take weeks I didn't have.

The reality: Initial parsing worked for simple cases. Edge cases appeared with nested parentheses, quoted strings, whitespace variations. Each fix introduced new bugs. Parser became fragile and hard to modify.

The irony: Avoided ANTLR because it would take weeks. String parsing also took weeks, except now I have fragile code instead of a proper parser.


## Future Enhancements

- Proper parser with lexer/tokenizer and AST-based parsing
- Logging and error reporting
- Multi-threaded table scans for large datasets
- Web-based query builder
- Support for MEDIAN, MODE, and other aggregates
- Cleaner code generation with templates 
- Modularize QueryGenerator


## References

1. Chatziantoniou, D., Ross, K. A. (1997). Querying Multiple Features of Groups in Relational Databases. Proceedings of the 22nd VLDB Conference, Mumbai, India. Department of Computer Science, Columbia University.

2. Chatziantoniou, D. Evaluation of Ad Hoc OLAP: In-Place Computation. Department of Computer Science, Stevens Institute of Technology.
