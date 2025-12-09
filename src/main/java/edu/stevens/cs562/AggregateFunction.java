package edu.stevens.cs562;

public class AggregateFunction {
    private String functionName;
    private String groupingVarName;
    private String attribute;

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