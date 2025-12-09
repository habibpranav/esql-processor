package edu.stevens.cs562;

/**
 * Converts EMFQuery to PhiOperator
 *
 * This is the critical conversion layer that maps the parsed ESQL query
 * into the canonical 6-operand Phi representation used by the algorithm.
 */
public class PhiConverter {

    /**
     * Convert EMFQuery → PhiOperator
     *
     * Key insight: predicates[0] = WHERE, predicates[1..n] = SUCH THAT for each grouping var
     */
    public static PhiOperator convert(EMFQuery query) {
        PhiOperator phi = new PhiOperator();

        // 1. S - Select attributes
        phi.selectAttributes = query.selectAttributes;

        // 2. n - Number of grouping variables
        phi.n = query.groupingVariableNames.size();

        // 3. V - Grouping attributes
        phi.groupingAttributes = query.groupingAttributes;

        // 4. F-VECT - Aggregate functions
        phi.fVect = query.fVectors;

        // 5.  (sigma) - Predicates [σ0, σ1, ..., σn]
        //    THIS IS THE KEY INSIGHT!
        //    σ0 = WHERE clause
        //    σ1 = SUCH THAT for grouping variable 1
        //    σ2 = SUCH THAT for grouping variable 2
        //    ...

        // σ0 = WHERE clause (used in SCAN 0 to filter rows)
        phi.predicates.add(query.whereConditions != null ? query.whereConditions : new ConditionExpression());

        // σ1..σn = SUCH THAT for each grouping variable (used in SCAN 1..n)
        for (String gvName : query.groupingVariableNames) {
            ConditionExpression suchThat = query.suchThatMap.get(gvName);
            phi.predicates.add(suchThat != null ? suchThat : new ConditionExpression());
        }

        // 6. G - HAVING clause
        phi.having = query.havingConditions != null ? query.havingConditions : new ConditionExpression();

        // Additional metadata
        phi.fromTable = query.fromTable;
        phi.groupingVariableNames = query.groupingVariableNames;

        return phi;
    }
}
