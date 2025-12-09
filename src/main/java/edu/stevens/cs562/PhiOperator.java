package edu.stevens.cs562;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the Phi operator with its 6 operands
 * This is the algebraic representation of an EMF query
 *
 * Phi(S, n, V, F, σ, G) where:
 * - S: projected attributes/expressions
 * - n: number of grouping variables
 * - V: grouping attributes
 * - F: aggregate functions
 * - σ: predicates (σ0=WHERE, σ1..σn=SUCH THAT for each grouping var)
 * - G: HAVING clause
 */
public class PhiOperator {
    // 1. S - List of projected attributes/expressions for query output
    public List<String> selectAttributes;

    // 2. n - Number of grouping variables
    public int n;

    // 3. V - List of grouping attributes (e.g., "cust", "prod")
    public List<String> groupingAttributes;

    // 4. F-VECT - List of aggregate functions
    public List<AggregateFunction> fVect;

    // 5. σ (sigma) - List of predicates [σ0, σ1, ..., σn]
    //    σ0 = WHERE clause (used in SCAN 0)
    //    σi = SUCH THAT for grouping variable i (used in SCAN i)
    public List<ConditionExpression> predicates;

    // 6. G - HAVING clause predicate
    public ConditionExpression having;

    // Additional metadata
    public String fromTable;
    public List<String> groupingVariableNames;  // ["x", "y", "z"]

    public PhiOperator() {
        this.selectAttributes = new ArrayList<>();
        this.n = 0;
        this.groupingAttributes = new ArrayList<>();
        this.fVect = new ArrayList<>();
        this.predicates = new ArrayList<>();
        this.having = new ConditionExpression();
        this.groupingVariableNames = new ArrayList<>();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("+----------------------------------------------------------------------------+\n");
        sb.append("|                     PHI OPERATOR (6 OPERANDS)                             |\n");
        sb.append("+----------------------------------------------------------------------------+\n");
        sb.append(" 1. S (Select):        ").append(selectAttributes).append("\n");
        sb.append(" 2. n (Grouping Vars): ").append(n).append("\n");
        sb.append(" 3. V (Group By):      ").append(groupingAttributes).append("\n");
        sb.append(" 4. F-VECT:            ").append(fVect).append("\n");
        sb.append(" 5. σ (Predicates):\n");
        for (int i = 0; i < predicates.size(); i++) {
            if (i == 0) {
                sb.append("    σ0 (WHERE):       ").append(predicates.get(i)).append("\n");
            } else {
                sb.append("    σ").append(i).append(" (").append(groupingVariableNames.get(i-1)).append("): ")
                  .append(predicates.get(i)).append("\n");
            }
        }
        sb.append(" 6. G (HAVING):        ").append(having).append("\n");
        sb.append("+----------------------------------------------------------------------------+\n");
        return sb.toString();
    }
}