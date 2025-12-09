package edu.stevens.cs562;

import java.util.*;

/**
 * EMFParser converts a raw ESQL/EMF query string into an EMFQuery object.
 * It extracts:
 *   SELECT
 *   FROM
 *   WHERE
 *   GROUP BY
 *   GROUPING VARIABLE NAMES
 *   SUCH THAT
 *   HAVING
 */
public class EMFParser {

    public EMFQuery parse(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            throw new RuntimeException("Empty query received.");
        }

        // Remove comments (lines starting with ---, //, #, or --)
        String[] lines = raw.split("\n");
        StringBuilder cleanedQuery = new StringBuilder();
        for (String line : lines) {
            String trimmedLine = line.trim();
            if (trimmedLine.startsWith("---") ||
                trimmedLine.startsWith("//") ||
                trimmedLine.startsWith("#") ||
                trimmedLine.startsWith("--")) {
                break;
            }
            cleanedQuery.append(line).append(" ");
        }
        raw = cleanedQuery.toString();

        // Extract string literals to preserve their case
        List<String> stringLiterals = new ArrayList<>();
        String temp = raw;
        int literalIndex = 0;

        while (temp.contains("'")) {
            int start = temp.indexOf("'");
            int end = temp.indexOf("'", start + 1);
            if (end == -1) break;

            String literal = temp.substring(start, end + 1);
            stringLiterals.add(literal);
            temp = temp.substring(0, start) + "___LITERAL_" + literalIndex + "___" + temp.substring(end + 1);
            literalIndex++;
        }

        String input = temp.toLowerCase().trim().replaceAll("\\s+", " ");

        for (int i = 0; i < stringLiterals.size(); i++) {
            input = input.replace("___literal_" + i + "___", stringLiterals.get(i));
        }

        Map<String, String> sections = extractSections(input);

        EMFQuery q = new EMFQuery();
        q.selectAttributes = parseSelect(sections.get("select"));
        q.fromTable = parseFrom(sections.get("from"));
        q.whereConditions = parseWhere(sections.get("where"));
        parseGroupBy(q, sections.get("group by"));
        q.suchThatMap = parseSuchThat(sections.get("such that"), q.groupingVariableNames);
        q.fVectors = parseFVectorsFromSelect(q.selectAttributes);
        extractAggregatesFromSuchThat(q.suchThatMap, q.fVectors);
        q.havingConditions = parseHaving(sections.get("having"));
        extractAggregatesFromHaving(q.havingConditions, q.fVectors);

        return q;
    }

    private Map<String, String> extractSections(String input) {
        String[] order = {"select", "from", "where", "group by", "such that", "having"};
        Map<String, String> map = new HashMap<>();

        for (int i = 0; i < order.length; i++) {
            String key = order[i];
            int start = input.indexOf(key);

            if (start == -1) {
                map.put(key, "");
                continue;
            }

            start = start + key.length();
            int end = input.length();

            for (int j = i + 1; j < order.length; j++) {
                int pos = input.indexOf(order[j], start);
                if (pos != -1) {
                    end = pos;
                    break;
                }
            }

            map.put(key, input.substring(start, end).trim());
        }

        return map;
    }

    private List<String> parseSelect(String s) {
        if (s == null || s.trim().isEmpty()) {
            return new ArrayList<>();
        }

        String[] parts = s.split(",");
        List<String> out = new ArrayList<>();

        for (String p : parts) {
            out.add(p.trim());
        }

        return out;
    }

    private String parseFrom(String s) {
        if (s == null || s.trim().isEmpty()) {
            throw new RuntimeException("FROM clause missing.");
        }
        return s.trim();
    }

    private ConditionExpression parseWhere(String s) {
        if (s == null || s.trim().isEmpty()) {
            return new ConditionExpression();
        }
        return parseConditionExpression(s.trim());
    }

    private void parseGroupBy(EMFQuery q, String s) {
        if (s == null || s.trim().isEmpty()) {
            q.groupingAttributes = new ArrayList<>();
            q.groupingVariableNames = new ArrayList<>();
            return;
        }

        String[] parts = s.split("[;:]");

        if (parts.length == 0) {
            throw new RuntimeException("Invalid GROUP BY syntax.");
        }

        String left = parts[0].trim();
        String[] attrs = left.split(",");
        q.groupingAttributes = new ArrayList<>();
        for (String a : attrs) q.groupingAttributes.add(a.trim());

        q.groupingVariableNames = new ArrayList<>();
        if (parts.length > 1) {
            String right = parts[1].trim();
            String[] vars = right.split(",");
            for (String v : vars) q.groupingVariableNames.add(v.trim());
        }
    }

    private HashMap<String, ConditionExpression> parseSuchThat(String s, List<String> gvNames) {
        HashMap<String, ConditionExpression> out = new HashMap<>();

        if (s == null || s.trim().isEmpty()) {
            return out;
        }

        s = s.trim();
        if (s.startsWith(":")) {
            s = s.substring(1).trim();
        }

        for (String gvName : gvNames) {
            String pattern = gvName + ".";
            int startIdx = s.indexOf(pattern);

            if (startIdx == -1) {
                continue;
            }

            int endIdx = s.length();
            for (String otherGV : gvNames) {
                if (otherGV.equals(gvName)) continue;
                int otherIdx = findNextGVStart(s, otherGV + ".", startIdx + pattern.length());
                if (otherIdx != -1 && otherIdx < endIdx) {
                    endIdx = otherIdx;
                }
            }

            String condition = s.substring(startIdx, endIdx).trim();

            if (condition.endsWith(",")) {
                condition = condition.substring(0, condition.length() - 1).trim();
            }

            String lowerCond = condition.toLowerCase();
            if (lowerCond.endsWith(" and")) {
                condition = condition.substring(0, condition.length() - 4).trim();
            } else if (lowerCond.endsWith(" or")) {
                condition = condition.substring(0, condition.length() - 3).trim();
            }

            ConditionExpression expr = parseConditionExpression(condition);
            out.put(gvName, expr);
        }

        return out;
    }

    private int findNextGVStart(String s, String pattern, int fromIndex) {
        int parenDepth = 0;
        int pos = fromIndex;

        while (pos < s.length()) {
            char c = s.charAt(pos);

            if (c == '(') {
                parenDepth++;
            } else if (c == ')') {
                parenDepth--;
            }

            if (parenDepth == 0 && pos + pattern.length() <= s.length()) {
                if (s.substring(pos, pos + pattern.length()).equals(pattern)) {
                    return pos;
                }
            }

            pos++;
        }

        return -1;
    }

    private List<AggregateFunction> parseFVectorsFromSelect(List<String> selectList) {
        List<AggregateFunction> list = new ArrayList<>();

        for (String item : selectList) {
            if (!item.contains("(")) continue;
            extractAggregatesFromExpression(item, list);
        }

        return list;
    }

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

    private void extractAggregatesFromSuchThat(Map<String, ConditionExpression> suchThatMap,
                                                 List<AggregateFunction> fVectors) {
        if (suchThatMap == null || suchThatMap.isEmpty()) {
            return;
        }

        for (ConditionExpression condExpr : suchThatMap.values()) {
            if (condExpr == null || condExpr.conditions.isEmpty()) {
                continue;
            }

            for (Condition cond : condExpr.conditions) {
                extractAggregatesFromExpression(cond.left, fVectors);
                extractAggregatesFromExpression(cond.right, fVectors);
            }
        }
    }

    private void extractAggregatesFromHaving(ConditionExpression havingExpr,
                                              List<AggregateFunction> fVectors) {
        if (havingExpr == null || havingExpr.conditions.isEmpty()) {
            return;
        }

        for (Condition cond : havingExpr.conditions) {
            extractAggregatesFromExpression(cond.left, fVectors);
            extractAggregatesFromExpression(cond.right, fVectors);
        }
    }

    private ConditionExpression parseHaving(String s) {
        if (s == null || s.trim().isEmpty()) {
            return new ConditionExpression();
        }

        s = s.trim();
        if (s.startsWith(":")) {
            s = s.substring(1).trim();
        }

        return parseConditionExpression(s.trim());
    }

    private ConditionExpression parseConditionExpression(String s) {
        ConditionExpression expr = new ConditionExpression();

        s = s.trim().replaceAll("\\s+", " ");

        String[] tokens = s.split("(?i)and|(?i)or");

        ArrayList<String> ops = new ArrayList<>();
        String temp = s.toLowerCase();

        while (true) {
            int andPos = temp.indexOf("and");
            int orPos  = temp.indexOf("or");

            if (andPos == -1 && orPos == -1) break;

            if (andPos != -1 && (orPos == -1 || andPos < orPos)) {
                ops.add("and");
                temp = temp.substring(andPos + 3);
            } else {
                ops.add("or");
                temp = temp.substring(orPos + 2);
            }
        }

        for (String token : tokens) {
            token = token.trim();
            if (!token.isEmpty()) {
                expr.conditions.add(parseSingleCondition(token));
            }
        }

        expr.operators = ops;
        return expr;
    }

    private Condition parseSingleCondition(String s) {
        boolean neg = false;

        if (s.startsWith("not ")) {
            neg = true;
            s = s.substring(4).trim();
        }

        if (s.contains(">=")) return split(s, ">=", neg);
        if (s.contains("<=")) return split(s, "<=", neg);
        if (s.contains("<>")) return split(s, "<>", neg);
        if (s.contains(">"))  return split(s, ">",  neg);
        if (s.contains("<"))  return split(s, "<",  neg);
        if (s.contains("="))  return split(s, "=",  neg);

        throw new RuntimeException("Invalid condition: " + s);
    }

    private Condition split(String s, String op, boolean neg) {
        String[] t = s.split(op);
        return new Condition(t[0].trim(), op, t[1].trim(), neg);
    }
}