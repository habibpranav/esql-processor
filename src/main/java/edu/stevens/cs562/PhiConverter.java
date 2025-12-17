package edu.stevens.cs562;

/**
 * Converts EMFQuery to PhiOperator
 *
 * Maps the parsed ESQL query into the 6-operand Phi representation:
 *   S, n, V, F-VECT, σ-VECT, G
 */
public class PhiConverter {

    /**
     * Convert EMFQuery → PhiOperator
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

        // 5. σ (sigma) - Predicates [σ0, σ1, ..., σn]
        //    σ0 = WHERE clause
        //    σ1..σn = SUCH THAT for each grouping variable
        phi.predicates.add(query.whereConditions != null ? query.whereConditions : new ConditionExpression());

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