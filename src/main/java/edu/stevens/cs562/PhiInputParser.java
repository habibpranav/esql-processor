package edu.stevens.cs562;

import java.util.*;

/**
 * Parses Phi operator format input
 * Example:
 * SELECT ATTRIBUTE(S):
 * cust, 1_sum_quant, 2_sum_quant, 3_sum_quant
 * NUMBER OF GROUPING VARIABLES(n):
 * 3
 * GROUPING ATTRIBUTES(V):
 * cust
 * F-VECT([F]):
 * 1_sum_quant, 1_avg_quant, 2_sum_quant, 3_sum_quant, 3_avg_quant
 * SELECT CONDITION-VECT([σ]):
 * 1.state='NY'
 * 2.state='NJ'
 * 3.state='CT'
 * HAVING_CONDITION(G):
 * 1_sum_quant > 2 * 2_sum_quant or 1_avg_quant > 3_avg_quant
 */
public class PhiInputParser {

    public EMFQuery parse(String input) {
        Map<String, String> sections = parseSections(input);

        // Parse FROM
        String fromTable = sections.getOrDefault("FROM", "sales").trim();

        // Parse WHERE (σ0)
        String whereCondition = sections.getOrDefault("WHERE", "").trim();

        // Parse SELECT ATTRIBUTE(S)
        String selectLine = sections.get("SELECT ATTRIBUTE(S)");
        List<String> selectAttributes = parseCommaSeparated(selectLine);

        // Parse NUMBER OF GROUPING VARIABLES(n)
        String nLine = sections.get("NUMBER OF GROUPING VARIABLES(n)");
        int n = Integer.parseInt(nLine.trim());

        // Parse GROUPING ATTRIBUTES(V)
        String vLine = sections.get("GROUPING ATTRIBUTES(V)");
        List<String> groupingAttributes = parseCommaSeparated(vLine);

        // Parse F-VECT([F])
        String fLine = sections.get("F-VECT([F])");
        List<String> fVect = parseCommaSeparated(fLine);

        // Parse SELECT CONDITION-VECT([σ])
        String sigmaLine = sections.get("SELECT CONDITION-VECT([σ])");
        List<String> predicates = parsePredicates(sigmaLine);

        // Parse HAVING_CONDITION(G)
        String havingLine = sections.getOrDefault("HAVING_CONDITION(G)", "").trim();

        // Convert to EMFQuery format
        return convertToEMFQuery(fromTable, whereCondition, selectAttributes, n, groupingAttributes, fVect, predicates, havingLine);
    }

    private Map<String, String> parseSections(String input) {
        Map<String, String> sections = new HashMap<>();
        String[] lines = input.split("\n");

        String currentSection = null;
        StringBuilder currentValue = new StringBuilder();

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty()) continue;

            if (line.endsWith(":")) {
                if (currentSection != null) {
                    sections.put(currentSection, currentValue.toString().trim());
                }
                currentSection = line.substring(0, line.length() - 1);
                currentValue = new StringBuilder();
            } else {
                if (currentValue.length() > 0) {
                    currentValue.append("\n");
                }
                currentValue.append(line);
            }
        }

        if (currentSection != null) {
            sections.put(currentSection, currentValue.toString().trim());
        }

        return sections;
    }

    private List<String> parseCommaSeparated(String line) {
        List<String> result = new ArrayList<>();
        if (line == null || line.trim().isEmpty()) return result;

        for (String item : line.split(",")) {
            result.add(item.trim());
        }
        return result;
    }

    private List<String> parsePredicates(String line) {
        List<String> result = new ArrayList<>();
        if (line == null || line.trim().isEmpty()) return result;

        for (String pred : line.split("\n")) {
            pred = pred.trim();
            if (!pred.isEmpty()) {
                result.add(pred);
            }
        }
        return result;
    }

    private EMFQuery convertToEMFQuery(String fromTable, String whereCondition, List<String> selectAttributes, int n,
                                       List<String> groupingAttributes,
                                       List<String> fVect,
                                       List<String> predicates,
                                       String havingCondition) {

        EMFQuery query = new EMFQuery();
        query.fromTable = fromTable;
        query.groupingAttributes = groupingAttributes;

        // Parse WHERE condition (σ0)
        if (whereCondition != null && !whereCondition.trim().isEmpty()) {
            query.whereConditions = parseConditionExpression(whereCondition);
        }

        // Extract grouping variable names from predicates (1.state, 2.state, etc.)
        query.groupingVariableNames = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            query.groupingVariableNames.add(String.valueOf(i));
        }

        // Parse F-VECT - can be in two formats:
        // Format 1: "1_sum_quant" (internal format)
        // Format 2: "sum(1.quant)" or "avg(X.quant)" (user-friendly format)
        query.fVectors = new ArrayList<>();
        for (String agg : fVect) {
            if (agg.contains("(") && agg.contains(")")) {
                // Format 2: avg(1.quant) or avg(X.quant)
                int open = agg.indexOf("(");
                int close = agg.lastIndexOf(")");
                String func = agg.substring(0, open).trim();
                String inside = agg.substring(open + 1, close).trim();
                String[] parts = inside.split("\\.");
                String varName = parts[0];
                String attr = parts.length > 1 ? parts[1] : "*";
                query.fVectors.add(new AggregateFunction(func, varName, attr));
            } else if (agg.contains("_")) {
                // Format 1: 1_sum_quant
                String[] parts = agg.split("_", 3);
                if (parts.length == 3) {
                    String varNum = parts[0];
                    String func = parts[1];
                    String attr = parts[2];
                    query.fVectors.add(new AggregateFunction(func, varNum, attr));
                }
            }
        }

        // SELECT attributes - accept both formats:
        // Format 1: "1_avg_quant" (internal)
        // Format 2: "avg(1.quant)" or "avg(X.quant)" (user-friendly)
        query.selectAttributes = new ArrayList<>();
        for (String attr : selectAttributes) {
            query.selectAttributes.add(attr.trim());
        }

        // Parse predicates - convert from Phi format to ESQL format
        query.suchThatMap = new HashMap<>();

        for (int i = 0; i < predicates.size(); i++) {
            String pred = predicates.get(i);
            String varNum = String.valueOf(i + 1);
            // Parse the predicate string into ConditionExpression
            query.suchThatMap.put(varNum, parseConditionExpression(pred));
        }

        // Parse HAVING condition
        if (havingCondition != null && !havingCondition.trim().isEmpty()) {
            query.havingConditions = parseConditionExpression(havingCondition);
        }

        return query;
    }

    private ConditionExpression parseConditionExpression(String predicate) {
        ConditionExpression expr = new ConditionExpression();
        if (predicate == null || predicate.trim().isEmpty()) {
            return expr;
        }

        // Simple parsing - split by AND/OR
        String[] parts = predicate.split("\\s+(and|or)\\s+", -1);
        List<String> operators = new ArrayList<>();

        // Extract operators
        String temp = predicate;
        while (temp.contains(" and ") || temp.contains(" or ")) {
            int andIdx = temp.indexOf(" and ");
            int orIdx = temp.indexOf(" or ");

            if (andIdx >= 0 && (orIdx < 0 || andIdx < orIdx)) {
                operators.add("and");
                temp = temp.substring(andIdx + 5);
            } else if (orIdx >= 0) {
                operators.add("or");
                temp = temp.substring(orIdx + 4);
            }
        }

        expr.operators = operators;

        // Parse each condition
        for (String part : parts) {
            part = part.trim();
            if (part.isEmpty()) continue;

            Condition cond = parseCondition(part);
            if (cond != null) {
                expr.conditions.add(cond);
            }
        }

        return expr;
    }

    private Condition parseCondition(String condStr) {
        // Parse conditions like: 1.state='NY' or 1_sum_quant > 2 * 2_sum_quant
        String[] operators = {">=", "<=", "<>", "!=", "=", ">", "<"};

        for (String op : operators) {
            int idx = condStr.indexOf(op);
            if (idx > 0) {
                String left = condStr.substring(0, idx).trim();
                String right = condStr.substring(idx + op.length()).trim();
                return new Condition(left, op, right, false);
            }
        }

        return null;
    }
}