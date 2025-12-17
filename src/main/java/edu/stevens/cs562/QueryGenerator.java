/**
 * ============================================================================
 * CS562 - Database Management Systems II
 * Fall 2025 - Stevens Institute of Technology
 *
 * Project: MF/EMF Query Processing Engine
 *
 * Authors: Pranav, Habib
 * Team: PH
 *
 * Description:
 * This program implements a query processing engine for Ad-Hoc OLAP queries
 * using the MF (Multi-Feature) and EMF (Extended Multi-Feature) query syntax.
 *
 * Given a Phi operator with 6 operands (S, n, V, F-VECT, σ, G), this engine
 * generates executable Java code that implements the EMF evaluation algorithm.
 * The generated code scans the sales table and computes aggregates in-memory
 * using the mf-structure data structure.
 *
 * Key Components:
 * - Parser: Reads EMF query input and extracts Phi operands
 * - QueryGenerator: Generates Java code from parsed query
 * - GeneratedQuery: Executable code that runs the EMF algorithm
 * ============================================================================
 */
package edu.stevens.cs562;

import java.io.*;
import java.sql.*;
import java.util.*;

public class QueryGenerator {
    private PhiOperator phi;
    // Column type cache: column name -> "int", "double", or "string"
    private Map<String, String> columnTypes = new HashMap<>();

    public QueryGenerator(PhiOperator phi) {
        this.phi = phi;
        loadColumnTypesFromDatabase();
    }

    /**
     * Dynamically load column types from the database using JDBC metadata.
     * Falls back to defaults if DB connection fails.
     */
    private void loadColumnTypesFromDatabase() {
        try {
            Properties props = new Properties();
            props.load(new FileInputStream("src/main/resources/db.properties"));
            String url = "jdbc:postgresql://" + props.getProperty("db.host") + ":" + props.getProperty("db.port") + "/" + props.getProperty("db.name");
            Connection conn = DriverManager.getConnection(url,
                props.getProperty("db.user"),
                props.getProperty("db.password"));

            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, phi.fromTable, null);

            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME").toLowerCase();
                int sqlType = columns.getInt("DATA_TYPE");

                // Map SQL types to our simple type system
                String type;
                switch (sqlType) {
                    case Types.INTEGER:
                    case Types.SMALLINT:
                    case Types.BIGINT:
                    case Types.TINYINT:
                        type = "int";
                        break;
                    case Types.FLOAT:
                    case Types.DOUBLE:
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                    case Types.REAL:
                        type = "double";
                        break;
                    case Types.DATE:
                        type = "date";
                        break;
                    default:
                        type = "string";
                }
                columnTypes.put(columnName, type);
            }

            columns.close();
            conn.close();

            System.out.println("Loaded column types from database: " + columnTypes);

        } catch (Exception e) {
            throw new RuntimeException("Failed to load column types from database: " + e.getMessage(), e);
        }
    }

    private String getColumnType(String column) {
        return columnTypes.getOrDefault(column.toLowerCase(), "string");
    }

    private boolean isNumericColumn(String column) {
        String type = getColumnType(column);
        return type.equals("int") || type.equals("double");
    }

    private boolean isDateColumn(String column) {
        return getColumnType(column).equals("date");
    }

    private String getJavaType(String column) {
        String type = getColumnType(column);
        switch (type) {
            case "int": return "int";
            case "double": return "double";
            case "date": return "java.sql.Date";
            default: return "String";
        }
    }

    public String generate() {
        try {
            InputStream is = getClass().getClassLoader().getResourceAsStream("QueryTemplate.txt");
            String template = new String(is.readAllBytes());
            return String.format(template, generateDeclarations(), generateScan0(), generateScans(), "        output();");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void writeToFile(String filename) throws IOException {
        FileWriter w = new FileWriter(filename);
        w.write(generate());
        w.close();
    }

    // ================== DECLARATIONS ==================

    private String generateDeclarations() {
        StringBuilder sb = new StringBuilder();

        // MFStruct class with proper types based on schema
        sb.append("    static class MFStruct {\n");
        for (String attr : phi.groupingAttributes) {
            String type = getJavaType(attr);
            sb.append("        " + type + " " + attr + ";\n");
        }
        for (AggregateFunction agg : phi.fVect) {
            String field = fieldName(agg);
            String type = getAggregateType(agg);
            sb.append("        " + type + " " + field + ";\n");
            if (agg.getFunctionName().equals("avg")) {
                sb.append("        int " + field + "_cnt;\n");
                sb.append("        double " + field + "_sum;\n");
            }
        }
        sb.append("    }\n\n");

        // Array and counter
        sb.append("    static MFStruct[] mf_struct = new MFStruct[10000];\n");
        sb.append("    static int NUM_OF_ENTRIES = 0;\n\n");

        // Lookup function with proper type comparisons
        sb.append("    static int lookup(" + params() + ") {\n");
        sb.append("        for (int i = 0; i < NUM_OF_ENTRIES; i++) {\n");
        sb.append("            if (" + lookupCondition() + ") return i;\n");
        sb.append("        }\n");
        sb.append("        return -1;\n");
        sb.append("    }\n\n");

        // Add function
        sb.append("    static void add(" + params() + ") {\n");
        sb.append("        mf_struct[NUM_OF_ENTRIES] = new MFStruct();\n");
        for (String attr : phi.groupingAttributes) {
            sb.append("        mf_struct[NUM_OF_ENTRIES]." + attr + " = " + attr + ";\n");
        }
        for (AggregateFunction agg : phi.fVect) {
            String field = fieldName(agg);
            String init = getInitValue(agg);
            sb.append("        mf_struct[NUM_OF_ENTRIES]." + field + " = " + init + ";\n");
            if (agg.getFunctionName().equals("avg")) {
                sb.append("        mf_struct[NUM_OF_ENTRIES]." + field + "_cnt = 0;\n");
                sb.append("        mf_struct[NUM_OF_ENTRIES]." + field + "_sum = 0.0;\n");
            }
        }
        sb.append("        NUM_OF_ENTRIES++;\n");
        sb.append("    }\n\n");

        // Output function
        sb.append("    static void output() {\n");
        sb.append("        System.out.println(\"\\n" + String.join(" | ", phi.selectAttributes) + "\");\n");
        sb.append("        System.out.println(\"" + "-".repeat(60) + "\");\n");
        sb.append("        for (int i = 0; i < NUM_OF_ENTRIES; i++) {\n");

        if (!phi.having.conditions.isEmpty()) {
            sb.append("            if (" + buildHaving() + ")\n    ");
        }
        sb.append("            System.out.println(" + buildPrintExpr() + ");\n");
        sb.append("        }\n");
        sb.append("    }");

        return sb.toString();
    }

    private String getAggregateType(AggregateFunction agg) {
        String func = agg.getFunctionName();
        if (func.equals("avg")) {
            return "double";
        } else if (func.equals("count")) {
            return "int";
        } else {
            // sum, min, max - depends on column type, but usually int for quant
            return "int";
        }
    }

    private String params() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < phi.groupingAttributes.size(); i++) {
            if (i > 0) sb.append(", ");
            String attr = phi.groupingAttributes.get(i);
            sb.append(getJavaType(attr) + " " + attr);
        }
        return sb.toString();
    }

    private String lookupCondition() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < phi.groupingAttributes.size(); i++) {
            if (i > 0) sb.append(" && ");
            String attr = phi.groupingAttributes.get(i);
            if (isNumericColumn(attr)) {
                sb.append("mf_struct[i]." + attr + " == " + attr);
            } else {
                // Both String and Date use .equals()
                sb.append("mf_struct[i]." + attr + ".equals(" + attr + ")");
            }
        }
        return sb.toString();
    }

    private String getInitValue(AggregateFunction agg) {
        switch (agg.getFunctionName()) {
            case "min": return "Integer.MAX_VALUE";
            case "max": return "Integer.MIN_VALUE";
            case "avg": return "0.0";
            default: return "0";
        }
    }

    private String buildPrintExpr() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < phi.selectAttributes.size(); i++) {
            if (i > 0) sb.append(" + \" | \" + ");
            String attr = phi.selectAttributes.get(i);

            if (phi.groupingAttributes.contains(attr)) {
                sb.append("mf_struct[i]." + attr);
            } else {
                String field = findField(attr);
                if (field != null) {
                    sb.append("mf_struct[i]." + field);
                } else {
                    sb.append(translateExpr(attr, "i"));
                }
            }
        }
        return sb.toString();
    }

    private String buildHaving() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < phi.having.conditions.size(); i++) {
            if (i > 0) {
                // Use the correct operator (and/or) from the parsed condition
                String logicalOp = (i - 1 < phi.having.operators.size()) ? phi.having.operators.get(i - 1) : "and";
                sb.append(logicalOp.equals("or") ? " || " : " && ");
            }
            Condition c = phi.having.conditions.get(i);
            String op = c.operator.equals("=") ? "==" : (c.operator.equals("<>") || c.operator.equals("!=") ? "!=" : c.operator);
            sb.append("(" + translateExpr(c.left, "i") + " " + op + " " + translateExpr(c.right, "i") + ")");
        }
        return sb.toString();
    }

    // ================== SCAN 0 ==================

    private String generateScan0() {
        StringBuilder sb = new StringBuilder();
        ConditionExpression where = phi.predicates.get(0);

        sb.append("        Statement s0 = conn.createStatement();\n");
        sb.append("        ResultSet r0 = s0.executeQuery(\"SELECT * FROM " + phi.fromTable + "\");\n");
        sb.append("        while (r0.next()) {\n");

        if (!where.conditions.isEmpty()) {
            sb.append("            if (" + buildWhere(where, "r0") + ") {\n    ");
        }

        sb.append("            if (lookup(" + rsArgs("r0") + ") == -1) add(" + rsArgs("r0") + ");\n");

        if (!where.conditions.isEmpty()) {
            sb.append("            }\n");
        }

        sb.append("        }\n");
        sb.append("        r0.close(); s0.close();");
        return sb.toString();
    }

    private String rsArgs(String rs) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < phi.groupingAttributes.size(); i++) {
            if (i > 0) sb.append(", ");
            String attr = phi.groupingAttributes.get(i);
            if (isNumericColumn(attr)) {
                sb.append(rs + ".getInt(\"" + attr + "\")");
            } else if (isDateColumn(attr)) {
                sb.append(rs + ".getDate(\"" + attr + "\")");
            } else {
                sb.append(rs + ".getString(\"" + attr + "\")");
            }
        }
        return sb.toString();
    }

    // ================== SCANS 1..N ==================

    private String generateScans() {
        StringBuilder sb = new StringBuilder();
        ConditionExpression where = phi.predicates.get(0);

        for (int i = 0; i < phi.n; i++) {
            String gv = phi.groupingVariableNames.get(i);
            String rs = "r" + (i + 1);
            ConditionExpression sigma = phi.predicates.get(i + 1);

            sb.append("\n        // SCAN " + (i + 1) + ": " + gv + "\n");
            sb.append("        Statement s" + (i + 1) + " = conn.createStatement();\n");
            sb.append("        ResultSet " + rs + " = s" + (i + 1) + ".executeQuery(\"SELECT * FROM " + phi.fromTable + "\");\n");
            sb.append("        while (" + rs + ".next()) {\n");

            if (!where.conditions.isEmpty()) {
                sb.append("            if (" + buildWhere(where, rs) + ") {\n");
            }

            sb.append("            for (int j = 0; j < NUM_OF_ENTRIES; j++) {\n");
            sb.append("                if (" + buildMatch(sigma, gv, rs) + ") {\n");

            for (AggregateFunction agg : phi.fVect) {
                if (agg.getGroupingVarName().equals(gv)) {
                    sb.append(generateAggUpdate(agg, rs));
                }
            }

            sb.append("                }\n");
            sb.append("            }\n");

            if (!where.conditions.isEmpty()) {
                sb.append("            }\n");
            }

            sb.append("        }\n");
            sb.append("        " + rs + ".close(); s" + (i + 1) + ".close();");
        }
        return sb.toString();
    }

    private String generateAggUpdate(AggregateFunction agg, String rs) {
        String f = fieldName(agg);
        String attr = agg.getAttribute();
        String indent = "                    ";

        switch (agg.getFunctionName()) {
            case "count":
                return indent + "mf_struct[j]." + f + "++;\n";
            case "sum":
                return indent + "mf_struct[j]." + f + " += " + rs + ".getInt(\"" + attr + "\");\n";
            case "max":
                return indent + "if (" + rs + ".getInt(\"" + attr + "\") > mf_struct[j]." + f + ") mf_struct[j]." + f + " = " + rs + ".getInt(\"" + attr + "\");\n";
            case "min":
                return indent + "if (mf_struct[j]." + f + " == Integer.MAX_VALUE || " + rs + ".getInt(\"" + attr + "\") < mf_struct[j]." + f + ") mf_struct[j]." + f + " = " + rs + ".getInt(\"" + attr + "\");\n";
            case "avg":
                return indent + "mf_struct[j]." + f + "_cnt++;\n" +
                       indent + "mf_struct[j]." + f + "_sum += " + rs + ".getInt(\"" + attr + "\");\n" +
                       indent + "mf_struct[j]." + f + " = mf_struct[j]." + f + "_sum / mf_struct[j]." + f + "_cnt;\n";
            default:
                return "";
        }
    }

    // ================== CONDITION BUILDING ==================

    private String buildWhere(ConditionExpression sigma, String rs) {
        if (sigma == null || sigma.conditions.isEmpty()) {
            return "true";  // No conditions means always true
        }

        List<String> parts = new ArrayList<>();
        for (int i = 0; i < sigma.conditions.size(); i++) {
            Condition c = sigma.conditions.get(i);
            String attr = c.left.contains(".") ? c.left.split("\\.")[1] : c.left;
            String op = c.operator.equals("=") ? "==" :
                       (c.operator.equals("<>") || c.operator.equals("!=")) ? "!=" : c.operator;

            String part = null;
            if (c.right.startsWith("'")) {
                String val = c.right.substring(1, c.right.length() - 1);
                if (c.operator.equals("<>") || c.operator.equals("!=")) {
                    part = "!" + rs + ".getString(\"" + attr + "\").equals(\"" + val + "\")";
                } else {
                    part = rs + ".getString(\"" + attr + "\").equals(\"" + val + "\")";
                }
            } else if (c.right.matches("-?\\d+")) {
                part = rs + ".getInt(\"" + attr + "\") " + op + " " + c.right;
            }

            if (part != null) {
                parts.add(part);
            }
        }

        if (parts.isEmpty()) {
            return "true";
        }

        // Build expression with proper AND/OR operators
        StringBuilder sb = new StringBuilder();
        sb.append("(").append(parts.get(0)).append(")");
        for (int i = 1; i < parts.size(); i++) {
            String logicalOp = (i - 1 < sigma.operators.size()) ?
                (sigma.operators.get(i - 1).equalsIgnoreCase("or") ? "||" : "&&") : "&&";
            sb.append(" ").append(logicalOp).append(" (").append(parts.get(i)).append(")");
        }
        return sb.toString();
    }

    private String buildMatch(ConditionExpression sigma, String gv, String rs) {
        List<String> parts = new ArrayList<>();
        List<String> operators = new ArrayList<>();

        for (int i = 0; i < sigma.conditions.size(); i++) {
            Condition c = sigma.conditions.get(i);
            if (!c.left.startsWith(gv + ".")) continue;

            String attr = c.left.split("\\.")[1];
            String right = c.right;
            String op = c.operator.equals("=") ? "==" :
                       (c.operator.equals("<>") || c.operator.equals("!=")) ? "!=" : c.operator;

            String part = null;

            // gv.attr = groupingAttr (e.g., x.prod = prod)
            if (phi.groupingAttributes.contains(right)) {
                if (isNumericColumn(right)) {
                    // Numeric grouping attribute - direct comparison
                    part = rs + ".getInt(\"" + attr + "\") " + op + " mf_struct[j]." + right;
                } else if (c.operator.equals("=")) {
                    part = rs + ".getString(\"" + attr + "\").equals(mf_struct[j]." + right + ")";
                } else if (c.operator.equals("<>") || c.operator.equals("!=")) {
                    // Not equals - use !equals() for strings
                    part = "!" + rs + ".getString(\"" + attr + "\").equals(mf_struct[j]." + right + ")";
                } else {
                    // Numeric comparison (<, >, <=, >=) on string that needs parsing
                    part = rs + ".getInt(\"" + attr + "\") " + c.operator + " Integer.parseInt(mf_struct[j]." + right + ")";
                }
            }
            // gv.attr = month+1 or month-1
            else if (right.matches(".*[+-]\\d+$") && !right.contains("(")) {
                for (String g : phi.groupingAttributes) {
                    if (right.startsWith(g)) {
                        String rest = right.substring(g.length()).trim();
                        if (isNumericColumn(g)) {
                            // Numeric grouping attribute - direct comparison
                            part = rs + ".getInt(\"" + attr + "\") " + op + " (mf_struct[j]." + g + " " + rest + ")";
                        } else {
                            part = rs + ".getInt(\"" + attr + "\") " + op + " (Integer.parseInt(mf_struct[j]." + g + ") " + rest + ")";
                        }
                        break;
                    }
                }
            }
            // gv.attr > avg(x.quant)
            else if (right.matches("(sum|avg|count|min|max)\\(.*\\)")) {
                for (AggregateFunction agg : phi.fVect) {
                    String pattern = agg.getFunctionName() + "(" + agg.getGroupingVarName() + "." + agg.getAttribute() + ")";
                    if (right.equals(pattern)) {
                        String field = fieldName(agg);
                        // For avg, also check that count > 0 (i.e., the aggregate was computed)
                        if (agg.getFunctionName().equals("avg")) {
                            parts.add("mf_struct[j]." + field + "_cnt > 0");
                            operators.add("&&");
                        }
                        part = rs + ".getInt(\"" + attr + "\") " + op + " mf_struct[j]." + field;
                        break;
                    }
                }
            }
            // gv.attr = 'NY'
            else if (right.startsWith("'")) {
                String val = right.substring(1, right.length() - 1);
                if (c.operator.equals("<>") || c.operator.equals("!=")) {
                    part = "!" + rs + ".getString(\"" + attr + "\").equals(\"" + val + "\")";
                } else {
                    part = rs + ".getString(\"" + attr + "\").equals(\"" + val + "\")";
                }
            }
            // gv.attr = 100
            else if (right.matches("-?\\d+")) {
                part = rs + ".getInt(\"" + attr + "\") " + op + " " + right;
            }

            if (part != null) {
                parts.add(part);
                // Track the logical operator (AND/OR) for this condition
                if (i < sigma.operators.size()) {
                    operators.add(sigma.operators.get(i).equalsIgnoreCase("or") ? "||" : "&&");
                }
            }
        }

        if (parts.isEmpty()) return "true";

        // Build the expression respecting AND/OR operators
        StringBuilder result = new StringBuilder();
        result.append("(").append(parts.get(0)).append(")");
        for (int i = 1; i < parts.size(); i++) {
            String logicalOp = (i - 1 < operators.size()) ? operators.get(i - 1) : "&&";
            result.append(" ").append(logicalOp).append(" (").append(parts.get(i)).append(")");
        }
        return result.toString();
    }

    // ================== HELPERS ==================

    private String fieldName(AggregateFunction agg) {
        int idx = phi.groupingVariableNames.indexOf(agg.getGroupingVarName());
        String gvNum = idx >= 0 ? String.valueOf(idx + 1) : agg.getGroupingVarName();
        String attr = agg.getAttribute().equals("*") ? "star" : agg.getAttribute();
        return agg.getFunctionName() + "_" + gvNum + "_" + attr;
    }

    private String findField(String expr) {
        for (AggregateFunction agg : phi.fVect) {
            String pattern = agg.getFunctionName() + "(" + agg.getGroupingVarName() + "." + agg.getAttribute() + ")";
            if (expr.equals(pattern)) return fieldName(agg);
        }
        return null;
    }

    private String translateExpr(String expr, String idx) {
        if (expr.matches("-?\\d+(\\.\\d+)?")) return expr;

        // Check if expression involves division - need double cast to avoid integer division
        boolean hasDiv = expr.contains("/");
        boolean hasNonIntAgg = false;
        if (hasDiv) {
            for (AggregateFunction agg : phi.fVect) {
                String pattern = agg.getFunctionName() + "(" + agg.getGroupingVarName() + "." + agg.getAttribute() + ")";
                if (expr.contains(pattern) && (agg.getFunctionName().equals("sum") || agg.getFunctionName().equals("avg"))) {
                    hasNonIntAgg = true;
                    break;
                }
            }
        }

        // Replace aggregate function patterns with field references
        for (AggregateFunction agg : phi.fVect) {
            String pattern = agg.getFunctionName() + "(" + agg.getGroupingVarName() + "." + agg.getAttribute() + ")";
            String replacement = (hasDiv && hasNonIntAgg ? "(double)" : "") + "mf_struct[" + idx + "]." + fieldName(agg);
            expr = expr.replace(pattern, replacement);
        }

        // Handle arithmetic expressions: "2 * sum(...)" or "sum(...) * 2"
        // The pattern replacement above should have replaced sum(...) with mf_struct reference
        // Now we just need to make sure the arithmetic operators are preserved
        // Java arithmetic operators (+, -, *, /) work directly, so no additional transformation needed

        return expr;
    }
}