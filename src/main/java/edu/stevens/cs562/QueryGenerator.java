package edu.stevens.cs562;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Generates executable Java code from PhiOperator
 */
public class QueryGenerator {

    private PhiOperator phi;

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
            String url = "jdbc:postgresql://localhost:5432/" + props.getProperty("db.name");
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
                    default:
                        type = "string";
                }
                columnTypes.put(columnName, type);
            }

            columns.close();
            conn.close();

            System.out.println("Loaded column types from database: " + columnTypes);

        } catch (Exception e) {
            System.err.println("Warning: Could not load column types from database, using defaults. " + e.getMessage());
            // Fallback to hardcoded defaults for sales table
            columnTypes.put("day", "int");
            columnTypes.put("month", "int");
            columnTypes.put("year", "int");
            columnTypes.put("quant", "int");
            columnTypes.put("cust", "string");
            columnTypes.put("prod", "string");
            columnTypes.put("state", "string");
            columnTypes.put("date", "string");
        }
    }

    private String getColumnType(String column) {
        return columnTypes.getOrDefault(column.toLowerCase(), "string");
    }

    private boolean isNumericColumn(String column) {
        String type = getColumnType(column);
        return type.equals("int") || type.equals("double");
    }

    public String generate() {
        String template = loadTemplate();
        return String.format(template,
            generateLookup(),
            generateAdd(),
            generateOutputFunction(),
            "", // Reserved for future use
            generateScan0(),
            generateScans(),
            generateOutputCall()
        );
    }

    private String loadTemplate() {
        try {
            InputStream is = getClass().getClassLoader().getResourceAsStream("QueryTemplate.txt");
            if (is == null) throw new RuntimeException("QueryTemplate.txt not found in resources");
            return new String(is.readAllBytes());
        } catch (Exception e) {
            throw new RuntimeException("Failed to load QueryTemplate.txt", e);
        }
    }

    private String generateLookup() {
        StringBuilder code = new StringBuilder();
        code.append("    // lookup function - searches for a group in mf-structure\n");
        code.append("    static Map<String, Object> lookup(");

        for (int i = 0; i < phi.groupingAttributes.size(); i++) {
            if (i > 0) code.append(", ");
            code.append("String ").append(phi.groupingAttributes.get(i));
        }

        code.append(") {\n");
        code.append("        String key = ");

        if (phi.groupingAttributes.size() == 1) {
            code.append(phi.groupingAttributes.get(0)).append(";\n");
        } else {
            for (int i = 0; i < phi.groupingAttributes.size(); i++) {
                if (i == 0) code.append(phi.groupingAttributes.get(i));
                else code.append(" + \"|\" + ").append(phi.groupingAttributes.get(i));
            }
            code.append(";\n");
        }

        code.append("        return mfStruct.get(key);\n");
        code.append("    }\n");

        return code.toString();
    }

    private String generateAdd() {
        StringBuilder code = new StringBuilder();
        code.append("    // add function - adds a new group to mf-structure\n");
        code.append("    static void add(");

        for (int i = 0; i < phi.groupingAttributes.size(); i++) {
            if (i > 0) code.append(", ");
            code.append("String ").append(phi.groupingAttributes.get(i));
        }

        code.append(") {\n");
        code.append("        Map<String, Object> row = new HashMap<>();\n");

        for (String attr : phi.groupingAttributes) {
            code.append("        row.put(\"").append(attr).append("\", ").append(attr).append(");\n");
        }

        for (AggregateFunction agg : phi.fVect) {
            String name = aggName(agg);
            if (agg.getFunctionName().equals("min")) {
                code.append("        row.put(\"").append(name).append("\", Integer.MAX_VALUE);\n");
            } else if (agg.getFunctionName().equals("avg")) {
                code.append("        row.put(\"").append(name).append("\", 0.0);\n");
            } else {
                code.append("        row.put(\"").append(name).append("\", 0);\n");
            }
        }

        code.append("        String key = ");

        if (phi.groupingAttributes.size() == 1) {
            code.append(phi.groupingAttributes.get(0)).append(";\n");
        } else {
            for (int i = 0; i < phi.groupingAttributes.size(); i++) {
                if (i == 0) code.append(phi.groupingAttributes.get(i));
                else code.append(" + \"|\" + ").append(phi.groupingAttributes.get(i));
            }
            code.append(";\n");
        }

        code.append("        mfStruct.put(key, row);\n");
        code.append("    }\n");

        return code.toString();
    }

    private String generateOutputFunction() {
        StringBuilder code = new StringBuilder();
        code.append("    // output function - prints the results\n");
        code.append("    static void output() {\n");
        code.append("        System.out.println(\"\\nResults:\");\n");
        code.append("        System.out.println(\"").append(String.join(" | ", phi.selectAttributes)).append("\");\n");
        code.append("        System.out.println(\"").append("-".repeat(60)).append("\");\n");
        code.append("        for (Map<String, Object> mfRow : mfStruct.values()) {\n");

        boolean hasCountInSelect = false;
        String countAggName = null;
        for (AggregateFunction agg : phi.fVect) {
            if (agg.getFunctionName().equals("count")) {
                for (String selectAttr : phi.selectAttributes) {
                    String aggStr = "count(" + agg.getGroupingVarName() + "." + agg.getAttribute() + ")";
                    if (selectAttr.equals(aggStr)) {
                        hasCountInSelect = true;
                        countAggName = aggName(agg);
                        break;
                    }
                }
            }
        }

        boolean needsFilter = !phi.having.conditions.isEmpty() || (hasCountInSelect && phi.having.conditions.isEmpty());

        if (needsFilter) {
            code.append("            ");
            if (!phi.having.conditions.isEmpty()) {
                String havingCond = buildHavingCondition(phi.having);
                code.append("if (").append(havingCond).append(") {\n");
            } else if (hasCountInSelect) {
                code.append("if (((Number)mfRow.get(\"").append(countAggName).append("\")).intValue() > 0) {\n");
            }
            buildPrintLine(code, "    ");
            code.append("            }\n");
        } else {
            buildPrintLine(code, "");
        }

        code.append("        }\n");
        code.append("    }\n");

        return code.toString();
    }

    private String generateScan0() {
        StringBuilder code = new StringBuilder();
        code.append("        // SCAN 0: Populate mf-structure with distinct groups\n");
        code.append("        Statement s0 = conn.createStatement();\n");

        String sql = "SELECT * FROM " + phi.fromTable;
        if (!phi.predicates.get(0).conditions.isEmpty()) {
            sql += " WHERE " + buildSQLCondition(phi.predicates.get(0));
        }

        code.append("        ResultSet r0 = s0.executeQuery(\"").append(sql).append("\");\n");
        code.append("        while (r0.next()) {\n");
        code.append("            Map<String, Object> row = lookup(");

        for (int i = 0; i < phi.groupingAttributes.size(); i++) {
            if (i > 0) code.append(", ");
            code.append("r0.getString(\"").append(phi.groupingAttributes.get(i)).append("\")");
        }

        code.append(");\n");
        code.append("            if (row == null) {\n");
        code.append("                add(");

        for (int i = 0; i < phi.groupingAttributes.size(); i++) {
            if (i > 0) code.append(", ");
            code.append("r0.getString(\"").append(phi.groupingAttributes.get(i)).append("\")");
        }

        code.append(");\n");
        code.append("            }\n");
        code.append("        }\n");
        code.append("        r0.close(); s0.close();\n\n");

        return code.toString();
    }

    private String generateScans() {
        StringBuilder code = new StringBuilder();

        for (int i = 0; i < phi.n; i++) {
            String gvName = phi.groupingVariableNames.get(i);
            ConditionExpression sigma = phi.predicates.get(i + 1);

            code.append("        // SCAN ").append(i + 1).append(": Grouping variable ").append(gvName)
                .append(" (σ").append(i + 1).append(")\n");

            String sql = "SELECT * FROM " + phi.fromTable;
            if (!phi.predicates.get(0).conditions.isEmpty()) {
                sql += " WHERE " + buildSQLCondition(phi.predicates.get(0));
            }

            code.append("        Statement s").append(i + 1).append(" = conn.createStatement();\n");
            code.append("        ResultSet r").append(i + 1).append(" = s").append(i + 1)
                .append(".executeQuery(\"").append(sql).append("\");\n");
            code.append("        while (r").append(i + 1).append(".next()) {\n");
            code.append("            for (Map<String, Object> mfRow : mfStruct.values()) {\n");

            String cond = buildFullCondition(sigma, gvName, "r" + (i + 1));
            if (!cond.equals("true")) {
                code.append("                if (").append(cond).append(") {\n");
            }

            for (AggregateFunction agg : phi.fVect) {
                if (agg.getGroupingVarName().equals(gvName)) {
                    code.append("                    updateAgg(mfRow, \"").append(aggName(agg))
                        .append("\", \"").append(agg.getFunctionName())
                        .append("\", ");

                    if (agg.getFunctionName().equals("count") || agg.getAttribute().equals("*")) {
                        code.append("1);\n");
                    } else {
                        code.append("Integer.parseInt(r").append(i + 1).append(".getString(\"")
                            .append(agg.getAttribute()).append("\")));\n");
                    }
                }
            }

            if (!cond.equals("true")) {
                code.append("                }\n");
            }
            code.append("            }\n");
            code.append("        }\n");
            code.append("        r").append(i + 1).append(".close(); s").append(i + 1).append(".close();\n\n");
        }

        return code.toString();
    }

    private String generateOutputCall() {
        return "        // Output results\n        output();\n";
    }

    private void buildPrintLine(StringBuilder code, String indent) {
        code.append(indent).append("            System.out.print(");
        for (int i = 0; i < phi.selectAttributes.size(); i++) {
            if (i > 0) code.append(" + \" | \" + ");

            String attr = phi.selectAttributes.get(i);

            if (attr.contains("/") || attr.contains("*") || attr.contains("+") || attr.contains("-")) {
                String expr = translateExpression(attr);
                code.append("(").append(expr).append(")");
            } else {
                boolean found = false;
                for (AggregateFunction agg : phi.fVect) {
                    String aggStr = agg.getFunctionName() + "(" + agg.getGroupingVarName() + "." + agg.getAttribute() + ")";
                    if (attr.equals(aggStr)) {
                        code.append("mfRow.get(\"").append(aggName(agg)).append("\")");
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    code.append("mfRow.get(\"").append(attr).append("\")");
                }
            }
        }
        code.append(");\n");
        code.append(indent).append("            System.out.println();\n");
    }

    private String translateExpression(String expr) {
        String result = expr;
        for (AggregateFunction agg : phi.fVect) {
            String aggStr = agg.getFunctionName() + "(" + agg.getGroupingVarName() + "." + agg.getAttribute() + ")";
            String replacement = "((Number)mfRow.get(\"" + aggName(agg) + "\")).doubleValue()";
            result = result.replace(aggStr, replacement);
        }
        return result;
    }

    private String buildSQLCondition(ConditionExpression expr) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < expr.conditions.size(); i++) {
            if (i > 0 && i - 1 < expr.operators.size()) {
                sb.append(" ").append(expr.operators.get(i - 1)).append(" ");
            }
            Condition c = expr.conditions.get(i);
            sb.append(c.left).append(" ").append(c.operator).append(" ").append(c.right);
        }
        return sb.toString();
    }

    private String buildFullCondition(ConditionExpression expr, String gvName, String rsVar) {
        List<String> parts = new ArrayList<>();

        for (Condition c : expr.conditions) {
            parts.add(buildSingleCondition(c, rsVar, gvName));
        }

        return parts.isEmpty() ? "true" : String.join(" && ", parts);
    }

    private String buildSingleCondition(Condition c, String rsVar, String gvName) {
        String left = c.left.contains(".") ? c.left.split("\\.")[1] : c.left;
        String leftCode = rsVar + ".getString(\"" + left + "\")";

        String right = c.right.trim();
        String rightCode;

        if (right.startsWith("'")) {
            rightCode = "\"" + right.substring(1, right.length() - 1) + "\"";
        } else if (right.matches("\\d+")) {
            rightCode = "\"" + right + "\"";
        } else if (phi.groupingAttributes.contains(right)) {
            rightCode = "String.valueOf(mfRow.get(\"" + right + "\"))";
        } else if (right.contains("(") && right.contains(")")) {
            rightCode = translateAggRef(right);
        } else if (right.contains("+") || right.contains("-") || right.contains("*") || right.contains("/")) {
            rightCode = "\"\" + " + translateArithmetic(right);
        } else {
            rightCode = rsVar + ".getString(\"" + right + "\")";
        }

        String op = c.operator;
        if (op.equals("=")) {
            return leftCode + ".equals(" + rightCode + ")";
        } else if (op.equals("<>") || op.equals("!=")) {
            return "!" + leftCode + ".equals(" + rightCode + ")";
        } else {
            boolean rightIsAggregate = right.contains("(") && right.contains(")");
            if (rightIsAggregate) {
                return "Integer.parseInt(" + leftCode + ") " + op + " " + rightCode;
            } else {
                return "Integer.parseInt(" + leftCode + ") " + op + " Integer.parseInt(" + rightCode + ")";
            }
        }
    }

    private String translateAggRef(String ref) {
        int open = ref.indexOf("(");
        int close = ref.lastIndexOf(")");
        String func = ref.substring(0, open).trim();
        String inside = ref.substring(open + 1, close).trim();

        if (inside.contains(".")) {
            String[] parts = inside.split("\\.");
            String gv = parts[0];
            String attr = parts[1];

            for (AggregateFunction agg : phi.fVect) {
                if (agg.getFunctionName().equals(func) &&
                    agg.getGroupingVarName().equals(gv) &&
                    agg.getAttribute().equals(attr)) {
                    return "((Number)mfRow.get(\"" + aggName(agg) + "\")).doubleValue()";
                }
            }
        }
        return ref;
    }

    private String translateArithmetic(String expr) {
        for (String op : new String[]{"+", "-", "*", "/"}) {
            int idx = expr.lastIndexOf(op);
            if (idx > 0 && idx < expr.length() - 1) {
                String left = expr.substring(0, idx).trim();
                String right = expr.substring(idx + 1).trim();

                String leftCode = phi.groupingAttributes.contains(left) ?
                    "(Integer.parseInt(String.valueOf(mfRow.get(\"" + left + "\"))))" :
                    left;

                return "(" + leftCode + " " + op + " " + right + ")";
            }
        }
        return expr;
    }

    private String buildHavingCondition(ConditionExpression expr) {
        if (expr.conditions.isEmpty()) return "true";

        StringBuilder sb = new StringBuilder();
        sb.append("(");

        for (int i = 0; i < expr.conditions.size(); i++) {
            Condition c = expr.conditions.get(i);
            String left = translateHavingOperand(c.left);
            String right = translateHavingOperand(c.right);
            String op = c.operator.equals("=") ? "==" : c.operator;
            sb.append(left).append(" ").append(op).append(" ").append(right);

            if (i < expr.conditions.size() - 1 && i < expr.operators.size()) {
                String logicalOp = expr.operators.get(i).equals("or") ? "||" : "&&";
                sb.append(") ").append(logicalOp).append(" (");
            }
        }

        sb.append(")");
        return sb.toString();
    }

    private String translateHavingOperand(String operand) {
        operand = operand.trim();

        if (operand.contains("+") || operand.contains("-") || operand.contains("*") || operand.contains("/")) {
            String result = operand;
            for (AggregateFunction agg : phi.fVect) {
                String aggStr = agg.getFunctionName() + "(" + agg.getGroupingVarName() + "." + agg.getAttribute() + ")";
                String valueType = agg.getFunctionName().equals("count") ? "intValue()" : "doubleValue()";
                String replacement = "((Number)mfRow.get(\"" + aggName(agg) + "\"))." + valueType;
                result = result.replace(aggStr, replacement);

                // Also handle Phi-format names like "1_avg_quant"
                String phiName = aggName(agg);
                result = result.replace(phiName, replacement);
            }
            return result;
        }

        if (operand.matches("\\d+")) return operand;

        // Check if operand is a Phi-format aggregate name like "1_avg_quant"
        for (AggregateFunction agg : phi.fVect) {
            String phiName = aggName(agg);
            if (operand.equals(phiName)) {
                String valueType = agg.getFunctionName().equals("count") ? "intValue()" : "doubleValue()";
                return "((Number)mfRow.get(\"" + phiName + "\"))." + valueType;
            }
        }

        if (operand.contains("(") && operand.contains(")")) {
            int open = operand.indexOf("(");
            int close = operand.lastIndexOf(")");
            String func = operand.substring(0, open).trim();
            String inside = operand.substring(open + 1, close).trim();

            if (inside.contains(".")) {
                String[] parts = inside.split("\\.");
                String gv = parts[0];
                String attr = parts[1];

                for (AggregateFunction agg : phi.fVect) {
                    if (agg.getFunctionName().equals(func) &&
                        agg.getGroupingVarName().equals(gv) &&
                        agg.getAttribute().equals(attr)) {
                        String valueType = agg.getFunctionName().equals("count") ? "intValue()" : "doubleValue()";
                        return "((Number)mfRow.get(\"" + aggName(agg) + "\"))." + valueType;
                    }
                }
            }
        }

        return operand;
    }

    private String aggName(AggregateFunction agg) {
        String gvName = agg.getGroupingVarName();

        // Check if grouping variable name is numeric (Phi format) or alphabetic (ESQL format)
        if (gvName.matches("\\d+")) {
            // Phi operator format: 1_sum_quant
            return gvName + "_" + agg.getFunctionName() + "_" + agg.getAttribute();
        } else {
            // ESQL format: sum_x_quant (for backward compatibility)
            // But convert to Phi format by finding the index
            int index = phi.groupingVariableNames.indexOf(gvName);
            if (index >= 0) {
                return (index + 1) + "_" + agg.getFunctionName() + "_" + agg.getAttribute();
            }
            // Fallback to old format if not found
            return agg.getFunctionName() + "_" + gvName + "_" + agg.getAttribute();
        }
    }

    public void writeToFile(String filename) throws IOException {
        try (FileWriter writer = new FileWriter(filename)) {
            writer.write(generate());
        }
    }
}