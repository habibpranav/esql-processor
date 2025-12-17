package edu.stevens.cs562;

/**
 * Represents an aggregate function in the F-VECT of a Phi operator.
 *
 * An aggregate function has three components:
 *   - functionName: The type of aggregation (sum, avg, count, max, min)
 *   - groupingVarName: Which grouping variable this aggregate belongs to (1, 2, 3... or X, Y, Z)
 *   - attribute: The column being aggregated (e.g., "quant")
 *
 * Example: For "sum(1.quant)" in F-VECT:
 *   - functionName = "sum"
 *   - groupingVarName = "1"
 *   - attribute = "quant"
 *
 * This is used to generate the MFStruct fields like "sum_1_quant" and
 * the corresponding aggregate update logic in the generated code.
 */
public class AggregateFunction {
    private String functionName;    // sum, avg, count, max, min
    private String groupingVarName; // 1, 2, 3 or X, Y, Z
    private String attribute;       // column name (e.g., quant)

    public AggregateFunction(String functionName, String groupingVarName, String attribute) {
        this.functionName = functionName;
        this.groupingVarName = groupingVarName;
        this.attribute = attribute;
    }

    public String getFunctionName() {
        return functionName;
    }

    public String getGroupingVarName() {
        return groupingVarName;
    }

    public String getAttribute() {
        return attribute;
    }

    @Override
    public String toString() {
        return functionName + "(" + groupingVarName + "." + attribute + ")";
    }
}