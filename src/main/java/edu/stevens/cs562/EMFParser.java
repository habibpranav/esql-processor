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
 *
 * It also parses:
 *   - Condition expressions (AND + OR + unary NOT)
 *   - Atomic conditions (>=, <=, <>, >, <, =)
 *   - Aggregate functions in SELECT
 */
public class EMFParser {


    // Raw String entry point
    public EMFQuery parse(String raw) {

        if (raw == null || raw.trim().isEmpty()) {
            throw new RuntimeException("Empty query received.");
        }

        // Convert the entered String in uppercase keywords and remove the extra whitespaces
        String input = raw.toUpperCase().trim().replaceAll("\\s+", " ");

        // Extract SELECT, FROM, WHERE, GROUP BY, SUCH THAT, HAVING
        Map<String, String> sections = extractSections(input);

        EMFQuery q = new EMFQuery();

        // SELECT
        q.selectAttributes = parseSelect(sections.get("SELECT"));

        // FROM
        q.fromTable = parseFrom(sections.get("FROM"));

        // WHERE
        q.whereConditions = parseWhere(sections.get("WHERE"));

        // GROUP BY (attributes + grouping variable names)
        parseGroupBy(q, sections.get("GROUP BY"));

        // SUCH THAT uses groupingVariableNames
        q.suchThatMap = parseSuchThat(sections.get("SUCH THAT"), q.groupingVariableNames);

        // Parse aggregate functions (F-VECTORS)
        q.fVectors = parseFVectorsFromSelect(q.selectAttributes);

        // HAVING
        q.havingConditions = parseHaving(sections.get("HAVING"));

        return q;
    }



    // Extract Query Sections
    private Map<String, String> extractSections(String input) {

        String[] order = {
                "SELECT",
                "FROM",
                "WHERE",
                "GROUP BY",
                "SUCH THAT",
                "HAVING"
        };

        Map<String, String> map = new HashMap<>();

        for (int i = 0; i < order.length; i++) {

            String key  = order[i];


            String next = (i + 1 < order.length) ? order[i + 1] : null;




            /**
             * `start` stores the starting position of the keys that match in our ESQL query.
             * It returns -1 if no matching value for the key is found.
             */
            int start = input.indexOf(key);

            if (start == -1) {
                map.put(key, "");
                continue;
            }

            /*
             * Since all the keys are just labels, we need the part that starts after these labels.
             * So we move our pointer to where the label ends.
             */
            start = start + key.length();

            int end;

            if (next == null) {

                end = input.length();
            } else {

                end = input.indexOf(next, start);


                if (end == -1) {
                    end = input.length();
                }
            }

            map.put(key, input.substring(start, end).trim());
        }

        return map;
    }






    // Parses SELECT

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






    // Parses FROM

    private String parseFrom(String s) {

        if (s == null || s.trim().isEmpty()) {
            throw new RuntimeException("FROM clause missing.");
        }

        return s.trim();
    }






    // Parses  WHERE

    private ConditionExpression parseWhere(String s) {

        if (s == null || s.trim().isEmpty()) {
            return new ConditionExpression();
        }

        return parseConditionExpression(s.trim());
    }





    // --------------------------
    // GROUP BY (attributes : GV names)
    // --------------------------
    private void parseGroupBy(EMFQuery q, String s) {

        if (s == null || s.trim().isEmpty()) {
            q.groupingAttributes = new ArrayList<>();
            q.groupingVariableNames = new ArrayList<>();
            return;
        }

        // Example:  GROUP BY cust, month: X, Y, Z
        String[] parts = s.split(":");

        if (parts.length == 0) {
            throw new RuntimeException("Invalid GROUP BY syntax.");
        }

        // LEFT SIDE = grouping attributes
        String left = parts[0].trim();
        String[] attrs = left.split(",");
        q.groupingAttributes = new ArrayList<>();
        for (String a : attrs) q.groupingAttributes.add(a.trim());

        // RIGHT SIDE = grouping variable names (X,Y,Z)
        q.groupingVariableNames = new ArrayList<>();
        if (parts.length > 1) {
            String right = parts[1].trim();
            String[] vars = right.split(",");
            for (String v : vars) q.groupingVariableNames.add(v.trim());
        }
    }





    // --------------------------
    // SUCH THAT section
    // --------------------------
    private HashMap<String, ConditionExpression> parseSuchThat(String s, List<String> gvNames) {

        HashMap<String, ConditionExpression> out = new HashMap<>();

        if (s == null || s.trim().isEmpty()) {
            return out;
        }

        // multiple GV conditions separated by commas
        String[] lines = s.split(",");

        for (String line : lines) {

            line = line.trim();
            if (line.isEmpty()) continue;

            // Example: X.CUST = CUST AND X.STATE = 'NY'
            int dot = line.indexOf(".");
            if (dot == -1) {
                throw new RuntimeException("Invalid SUCH THAT condition: " + line);
            }

            // Extract GV name
            String gvName = line.substring(0, dot).trim();

            // Validate GV exists
            if (!gvNames.contains(gvName)) {
                throw new RuntimeException("Grouping variable " + gvName + " not declared in GROUP BY.");
            }

            // Parse entire condition
            ConditionExpression expr = parseConditionExpression(line);

            out.put(gvName, expr);
        }

        return out;
    }





    // --------------------------
    // PARSE F-VECTORS (AGGREGATES)
    // --------------------------
    private List<AggregateFunction> parseFVectorsFromSelect(List<String> selectList) {

        List<AggregateFunction> list = new ArrayList<>();

        for (String item : selectList) {

            if (!item.contains("(")) continue;

            String fn = item.substring(0, item.indexOf("(")).trim();

            String inside = item.substring(
                    item.indexOf("(") + 1,
                    item.indexOf(")")
            ).trim();

            String[] parts = inside.split("\\.");

            String gv  = parts[0].trim();
            String att = parts[1].trim();

            list.add(new AggregateFunction(fn, gv, att));
        }

        return list;
    }





    // --------------------------
    // HAVING
    // --------------------------
    private ConditionExpression parseHaving(String s) {

        if (s == null || s.trim().isEmpty()) {
            return new ConditionExpression();
        }

        return parseConditionExpression(s.trim());
    }





    // --------------------------
    // PARSE CONDITION EXPRESSIONS (AND / OR)
    // --------------------------
    private ConditionExpression parseConditionExpression(String s) {

        ConditionExpression expr = new ConditionExpression();

        s = s.trim().replaceAll("\\s+", " ");

        String[] tokens = s.split("(?i)AND|(?i)OR");

        List<String> ops = new ArrayList<>();
        String temp = s.toUpperCase();

        while (true) {
            int andPos = temp.indexOf("AND");
            int orPos  = temp.indexOf("OR");

            if (andPos == -1 && orPos == -1) break;

            if (andPos != -1 && (orPos == -1 || andPos < orPos)) {
                ops.add("AND");
                temp = temp.substring(andPos + 3);
            } else {
                ops.add("OR");
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





    // --------------------------
    // PARSE A SINGLE ATOMIC CONDITION
    // --------------------------
    private Condition parseSingleCondition(String s) {

        boolean neg = false;

        if (s.startsWith("NOT ")) {
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