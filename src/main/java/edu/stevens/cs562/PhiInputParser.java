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
 * HAVING_CONDITION(G) 𝛔
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

        // Extract grouping variable names from F-VECT, select attrs, or predicates
        // Supports both numbered (1, 2, 3) and named (X, Y, Z) formats
        Set<String> varNamesSet = new LinkedHashSet<>();

        // First try to extract ALL var names from F-VECT (handles sum(x.quant)/sum(y.quant))
        for (String agg : fVect) {
            extractAllVarNames(agg, varNamesSet);
        }

        // Also extract from select attributes
        for (String attr : selectAttributes) {
            extractAllVarNames(attr, varNamesSet);
        }

        // If we didn't get enough, extract from predicates
        if (varNamesSet.size() < n) {
            for (String pred : predicates) {
                String varName = extractVarNameFromPredicate(pred);
                if (varName != null) {
                    varNamesSet.add(varName);
                }
            }
        }

        // Convert to list, maintaining order
        query.groupingVariableNames = new ArrayList<>(varNamesSet);

        // If still not enough, fall back to numbered
        while (query.groupingVariableNames.size() < n) {
            query.groupingVariableNames.add(String.valueOf(query.groupingVariableNames.size() + 1));
        }

        // Parse F-VECT using same logic as EMFParser.extractAggregatesFromExpression
        query.fVectors = new ArrayList<>();
        for (String item : fVect) {
            extractAggregatesFromExpression(item, query.fVectors);
        }
        // Also extract from select attributes (for expressions like sum(x.quant)/sum(y.quant))
        for (String item : selectAttributes) {
            extractAggregatesFromExpression(item, query.fVectors);
        }

        // SELECT attributes - accept both formats:
        // Format 1: "1_avg_quant" (internal)
        // Format 2: "avg(1.quant)" or "avg(X.quant)" (user-friendly)
        query.selectAttributes = new ArrayList<>();
        for (String attr : selectAttributes) {
            query.selectAttributes.add(attr.trim());
        }

        // Parse predicates - extract the variable name FROM the predicate itself
        query.suchThatMap = new HashMap<>();

        for (int i = 0; i < predicates.size(); i++) {
            String pred = predicates.get(i);
            // Extract the variable name from the predicate (e.g., "x.prod=prod" -> "x")
            String varName = extractVarNameFromPredicate(pred);
            if (varName == null) {
                // Fallback to position-based
                varName = (i < query.groupingVariableNames.size())
                    ? query.groupingVariableNames.get(i)
                    : String.valueOf(i + 1);
            }
            // Parse the predicate string into ConditionExpression
            query.suchThatMap.put(varName, parseConditionExpression(pred));
        }

        // Also rebuild groupingVariableNames from predicates to maintain correct order
        query.groupingVariableNames = new ArrayList<>();
        for (String pred : predicates) {
            String varName = extractVarNameFromPredicate(pred);
            if (varName != null && !query.groupingVariableNames.contains(varName)) {
                query.groupingVariableNames.add(varName);
            }
        }
        // Ensure we have n variables
        while (query.groupingVariableNames.size() < n) {
            query.groupingVariableNames.add(String.valueOf(query.groupingVariableNames.size() + 1));
        }

        // Parse HAVING condition
        if (havingCondition != null && !havingCondition.trim().isEmpty()) {
            query.havingConditions = parseConditionExpression(havingCondition);
        }

        return query;
    }

    // Same logic as EMFParser.parseConditionExpression
    private ConditionExpression parseConditionExpression(String s) {
        ConditionExpression expr = new ConditionExpression();
        if (s == null || s.trim().isEmpty()) {
            return expr;
        }

        s = s.trim().replaceAll("\\s+", " ");

        // Case-insensitive split by AND/OR
        String[] tokens = s.split("(?i)\\s+and\\s+|(?i)\\s+or\\s+");

        ArrayList<String> ops = new ArrayList<>();
        String temp = s.toLowerCase();

        while (true) {
            int andPos = temp.indexOf(" and ");
            int orPos = temp.indexOf(" or ");

            if (andPos == -1 && orPos == -1) break;

            if (andPos != -1 && (orPos == -1 || andPos < orPos)) {
                ops.add("and");
                temp = temp.substring(andPos + 5);
            } else {
                ops.add("or");
                temp = temp.substring(orPos + 4);
            }
        }

        for (String token : tokens) {
            token = token.trim();
            if (!token.isEmpty()) {
                Condition cond = parseSingleCondition(token);
                if (cond != null) {
                    expr.conditions.add(cond);
                }
            }
        }

        expr.operators = ops;
        return expr;
    }

    // Same logic as EMFParser.parseSingleCondition
    private Condition parseSingleCondition(String s) {
        boolean neg = false;

        if (s.toLowerCase().startsWith("not ")) {
            neg = true;
            s = s.substring(4).trim();
        }

        // Order matters: check multi-char operators first
        if (s.contains(">=")) return split(s, ">=", neg);
        if (s.contains("<=")) return split(s, "<=", neg);
        if (s.contains("<>")) return split(s, "<>", neg);
        if (s.contains("!=")) return split(s, "!=", neg);
        if (s.contains(">"))  return split(s, ">",  neg);
        if (s.contains("<"))  return split(s, "<",  neg);
        if (s.contains("="))  return split(s, "=",  neg);

        // No operator found - return null
        return null;
    }

    private Condition split(String s, String op, boolean neg) {
        String[] t = s.split(java.util.regex.Pattern.quote(op), 2);
        if (t.length == 2) {
            return new Condition(t[0].trim(), op, t[1].trim(), neg);
        }
        return null;
    }

    // Extract ALL variable names from expression - handles complex expressions like sum(x.quant)/sum(y.quant)
    private String extractVarName(String expr) {
        // Use same logic as extractAggregatesFromExpression but just return first var name found
        int pos = 0;
        while (pos < expr.length()) {
            int openParen = expr.indexOf("(", pos);
            if (openParen == -1) break;

            int fnStart = openParen - 1;
            while (fnStart >= 0 && Character.isLetterOrDigit(expr.charAt(fnStart))) {
                fnStart--;
            }
            fnStart++;

            if (fnStart >= openParen) {
                pos = openParen + 1;
                continue;
            }

            String fn = expr.substring(fnStart, openParen).trim();

            if (fn.matches("(sum|avg|count|min|max)")) {
                int closeParen = openParen + 1;
                int parenCount = 1;
                while (closeParen < expr.length() && parenCount > 0) {
                    if (expr.charAt(closeParen) == '(') parenCount++;
                    if (expr.charAt(closeParen) == ')') parenCount--;
                    closeParen++;
                }

                String inside = expr.substring(openParen + 1, closeParen - 1).trim();
                if (inside.contains(".")) {
                    return inside.split("\\.")[0].trim();
                }
                pos = closeParen;
            } else {
                pos = openParen + 1;
            }
        }

        // Fallback for underscore format: 1_sum_quant
        if (expr.contains("_")) {
            String[] parts = expr.split("_", 3);
            if (parts.length >= 1) {
                return parts[0].trim();
            }
        }
        return null;
    }

    // Extract ALL variable names from expression for building groupingVariableNames
    private void extractAllVarNames(String expr, Set<String> varNames) {
        int pos = 0;
        while (pos < expr.length()) {
            int openParen = expr.indexOf("(", pos);
            if (openParen == -1) break;

            int fnStart = openParen - 1;
            while (fnStart >= 0 && Character.isLetterOrDigit(expr.charAt(fnStart))) {
                fnStart--;
            }
            fnStart++;

            if (fnStart >= openParen) {
                pos = openParen + 1;
                continue;
            }

            String fn = expr.substring(fnStart, openParen).trim();

            if (fn.matches("(sum|avg|count|min|max)")) {
                int closeParen = openParen + 1;
                int parenCount = 1;
                while (closeParen < expr.length() && parenCount > 0) {
                    if (expr.charAt(closeParen) == '(') parenCount++;
                    if (expr.charAt(closeParen) == ')') parenCount--;
                    closeParen++;
                }

                String inside = expr.substring(openParen + 1, closeParen - 1).trim();
                if (inside.contains(".")) {
                    varNames.add(inside.split("\\.")[0].trim());
                }
                pos = closeParen;
            } else {
                pos = openParen + 1;
            }
        }
    }

    // Extract variable name from predicate like "X.cust=cust" or "1.state='NY'"
    private String extractVarNameFromPredicate(String pred) {
        // Look for pattern like "X." or "1." at the start
        int dotIdx = pred.indexOf(".");
        if (dotIdx > 0) {
            String potential = pred.substring(0, dotIdx).trim();
            // Make sure it's just the variable name (no spaces or operators before it)
            if (potential.matches("[a-zA-Z0-9]+")) {
                return potential;
            }
        }
        return null;
    }

    // Same as EMFParser.extractAggregatesFromExpression
    private void extractAggregatesFromExpression(String expr, List<AggregateFunction> list) {
        int pos = 0;
        while (pos < expr.length()) {
            int openParen = expr.indexOf("(", pos);
            if (openParen == -1) break;

            int fnStart = openParen - 1;
            while (fnStart >= 0 && Character.isLetterOrDigit(expr.charAt(fnStart))) {
                fnStart--;
            }
            fnStart++;

            if (fnStart >= openParen) {
                pos = openParen + 1;
                continue;
            }

            String fn = expr.substring(fnStart, openParen).trim();

            if (fn.matches("(sum|avg|count|min|max)")) {
                int closeParen = openParen + 1;
                int parenCount = 1;
                while (closeParen < expr.length() && parenCount > 0) {
                    if (expr.charAt(closeParen) == '(') parenCount++;
                    if (expr.charAt(closeParen) == ')') parenCount--;
                    closeParen++;
                }

                String inside = expr.substring(openParen + 1, closeParen - 1).trim();

                // Handle count(*) specially
                if (inside.equals("*")) {
                    pos = closeParen;
                    continue;
                }

                if (inside.contains(".")) {
                    String[] parts = inside.split("\\.");
                    String gv = parts[0].trim();
                    String att = parts[1].trim();

                    boolean exists = false;
                    for (AggregateFunction existing : list) {
                        if (existing.getFunctionName().equals(fn) &&
                            existing.getGroupingVarName().equals(gv) &&
                            existing.getAttribute().equals(att)) {
                            exists = true;
                            break;
                        }
                    }

                    if (!exists) {
                        list.add(new AggregateFunction(fn, gv, att));
                    }
                }

                pos = closeParen;
            } else {
                pos = openParen + 1;
            }
        }
    }
}