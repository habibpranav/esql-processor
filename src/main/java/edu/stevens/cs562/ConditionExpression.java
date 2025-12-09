package edu.stevens.cs562;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a chain of conditions connected by AND/OR operators
 */
public class ConditionExpression {
    public List<Condition> conditions;
    public List<String> operators;  // "AND" or "OR"

    public ConditionExpression() {
        this.conditions = new ArrayList<>();
        this.operators = new ArrayList<>();
    }

    @Override
    public String toString() {
        if (conditions.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < conditions.size(); i++) {
            if (i > 0 && i - 1 < operators.size()) {
                sb.append(" ").append(operators.get(i - 1)).append(" ");
            }
            sb.append(conditions.get(i).toString());
        }
        return sb.toString();
    }
}