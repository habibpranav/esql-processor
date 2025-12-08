package edu.stevens.cs562;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Generates executable Java code that implements the EMF evaluation algorithm
 *
 * This class is DYNAMIC - it generates code based on the parsed EMFQuery
 *
 * Generated code structure:
 * 1. Create mf-structure with schema
 * 2. Scan 0: Populate mf-structure with distinct grouping attribute values
 * 3. Scan 1 to n: For each grouping variable, scan the table and update aggregates
 * 4. Apply HAVING clause
 * 5. Output results
 */
public class QueryGenerator {

    private EMFQuery query;
    private StringBuilder code;

    public QueryGenerator(EMFQuery query) {
        this.query = query;
        this.code = new StringBuilder();
    }

    /**
     * Generate the complete Java program
     */
    public String generate() {
        code = new StringBuilder();

        generateImports();
        generateClassHeader();
        generateMainMethod();
        generateHelperMethods();
        generateClassFooter();

        return code.toString();
    }

    private void generateImports() {
        code.append("import java.sql.*;\n");
        code.append("import java.util.*;\n");
        code.append("import java.io.*;\n\n");
    }

    private void generateClassHeader() {
        code.append("public class GeneratedQuery {\n\n");
    }

    private void generateClassFooter() {
        code.append("}\n");
    }

    private void generateMainMethod() {
        code.append("    public static void main(String[] args) {\n");
        code.append("        try {\n");

        // Database connection
        generateDatabaseConnection();

        // Create mf-structure
        generateMFStructureCreation();

        // Scan 0: Populate with distinct grouping values
        generateScan0();

        // OPTIMIZATION 2: Dependency Analysis - Combine independent scans
        List<List<String>> scanGroups = groupVariablesByDependency();
        int scanNum = 1;
        for (List<String> group : scanGroups) {
            if (group.size() == 1) {
                generateScanForGroupingVariable(scanNum++, group.get(0));
            } else {
                generateCombinedScan(scanNum++, group);
            }
        }

        // Apply HAVING and output
        generateHavingAndOutput();

        code.append("        } catch (Exception e) {\n");
        code.append("            e.printStackTrace();\n");
        code.append("        }\n");
        code.append("    }\n\n");
    }

    private void generateDatabaseConnection() {
        code.append("            // Load database properties\n");
        code.append("            Properties dbProps = new Properties();\n");
        code.append("            try (InputStream input = new FileInputStream(\"src/main/resources/db.properties\")) {\n");
        code.append("                dbProps.load(input);\n");
        code.append("            }\n");
        code.append("            \n");
        code.append("            // Connect to PostgreSQL\n");
        code.append("            String dbName = dbProps.getProperty(\"db.name\");\n");
        code.append("            String user = dbProps.getProperty(\"db.user\");\n");
        code.append("            String password = dbProps.getProperty(\"db.password\");\n");
        code.append("            String url = \"jdbc:postgresql://localhost:5432/\" + dbName;\n");
        code.append("            Connection conn = DriverManager.getConnection(url, user, password);\n\n");
    }

    private void generateMFStructureCreation() {
        code.append("            // Create mf-structure (in-memory)\n");
        code.append("            // OPTIMIZATION 3: For very large datasets, partition mf-structure into rounds\n");
        code.append("            // Current implementation: Load entire mf-structure in memory\n");
        code.append("            // Future: Process in partitions (e.g., by hash of grouping key % partition_count)\n");
        code.append("            Map<String, Map<String, Object>> mfStruct = new HashMap<>();\n\n");
    }

    private void generateScan0() {
        code.append("            // SCAN 0: Populate mf-structure with distinct grouping values\n");
        code.append("            Statement stmt0 = conn.createStatement();\n");

        String selectQuery = buildSelectQuery();

        code.append("            ResultSet rs0 = stmt0.executeQuery(\"" + selectQuery + "\");\n");
        code.append("            \n");
        code.append("            while (rs0.next()) {\n");

        // Create key from grouping attributes
        code.append("                String key = \"\";\n");
        for (int i = 0; i < query.groupingAttributes.size(); i++) {
            String attr = query.groupingAttributes.get(i);
            if (i > 0) {
                code.append("                key += \"|\" + rs0.getString(\"" + attr + "\");\n");
            } else {
                code.append("                key = rs0.getString(\"" + attr + "\");\n");
            }
        }

        code.append("                \n");
        code.append("                if (!mfStruct.containsKey(key)) {\n");
        code.append("                    Map<String, Object> row = new HashMap<>();\n");

        // Initialize grouping attributes
        for (String attr : query.groupingAttributes) {
            code.append("                    row.put(\"" + attr + "\", rs0.getString(\"" + attr + "\"));\n");
        }

        // Initialize all aggregates to 0
        for (AggregateFunction agg : query.fVectors) {
            String aggName = formatAggregateName(agg);
            // Initialize AVG to 0.0 (double), others to 0 (int)
            if (agg.getFunctionName().equals("avg")) {
                code.append("                    row.put(\"" + aggName + "\", 0.0);\n");
            } else {
                code.append("                    row.put(\"" + aggName + "\", 0);\n");
            }
        }

        code.append("                    mfStruct.put(key, row);\n");
        code.append("                }\n");
        code.append("            }\n");
        code.append("            rs0.close();\n");
        code.append("            stmt0.close();\n\n");

        code.append("            System.out.println(\"MF-Structure initialized with \" + mfStruct.size() + \" groups\");\n\n");

        // OPTIMIZATION 1: Build indexes
        generateIndexBuilding();
    }

    private void generateIndexBuilding() {
        code.append("            // OPTIMIZATION 1: Build indexes on grouping attributes for faster lookup\n");

        // Analyze which attributes need indexes
        Set<String> indexedAttrs = new HashSet<>();
        for (String gvName : query.groupingVariableNames) {
            ConditionExpression cond = query.suchThatMap.get(gvName);
            if (cond != null) {
                for (Condition c : cond.conditions) {
                    // Check if condition is like "x.attr = attr" (equality with grouping attribute)
                    if (c.operator.equals("=") && !c.left.contains("(") && !c.right.contains("(")) {
                        // Extract attribute names
                        String leftAttr = c.left.contains(".") ? c.left.split("\\.")[1] : c.left;
                        String rightAttr = c.right.contains(".") ? c.right.split("\\.")[1] : c.right;

                        // If one side is a grouping attribute, index it
                        if (query.groupingAttributes.contains(rightAttr)) {
                            indexedAttrs.add(rightAttr);
                        }
                        if (query.groupingAttributes.contains(leftAttr)) {
                            indexedAttrs.add(leftAttr);
                        }
                    }
                }
            }
        }

        // Generate index structures
        for (String attr : indexedAttrs) {
            code.append("            Map<String, List<Map<String, Object>>> index_" + attr + " = new HashMap<>();\n");
            code.append("            for (Map<String, Object> row : mfStruct.values()) {\n");
            code.append("                String val = String.valueOf(row.get(\"" + attr + "\"));\n");
            code.append("                index_" + attr + ".computeIfAbsent(val, k -> new ArrayList<>()).add(row);\n");
            code.append("            }\n");
            code.append("            System.out.println(\"Built index on '" + attr + "' with \" + index_" + attr + ".size() + \" distinct values\");\n");
        }
        code.append("\n");
    }

    private String buildSelectQuery() {
        String selectQuery = "select * from " + query.fromTable;
        if (query.whereConditions != null && !query.whereConditions.conditions.isEmpty()) {
            String whereClause = generateSQLWhereClause(query.whereConditions);
            selectQuery += " where " + whereClause;
        }
        return selectQuery;
    }

    private String generateSQLWhereClause(ConditionExpression expr) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < expr.conditions.size(); i++) {
            if (i > 0 && i - 1 < expr.operators.size()) {
                result.append(" ").append(expr.operators.get(i - 1)).append(" ");
            }
            Condition cond = expr.conditions.get(i);
            result.append(cond.left).append(" ").append(cond.operator).append(" ").append(cond.right);
        }
        return result.toString();
    }

    private void generateScanForGroupingVariable(int scanNum, String gvName) {
        code.append("            // SCAN " + scanNum + ": Process grouping variable " + gvName + "\n");
        code.append("            Statement stmt" + scanNum + " = conn.createStatement();\n");

        String selectQuery = buildSelectQuery();
        code.append("            ResultSet rs" + scanNum + " = stmt" + scanNum + ".executeQuery(\"" + selectQuery + "\");\n");
        code.append("            \n");
        code.append("            while (rs" + scanNum + ".next()) {\n");

        // Get the such that condition for this grouping variable
        ConditionExpression suchThatCond = query.suchThatMap.get(gvName);

        // OPTIMIZATION 1: Use index if possible
        String indexAttr = findIndexableAttribute(suchThatCond);

        if (indexAttr != null) {
            // Use index lookup
            code.append("                // OPTIMIZATION: Use index on '" + indexAttr + "' for faster lookup\n");
            code.append("                String lookupVal = rs" + scanNum + ".getString(\"" + indexAttr + "\");\n");
            code.append("                List<Map<String, Object>> candidates = index_" + indexAttr + ".get(lookupVal);\n");
            code.append("                if (candidates != null) {\n");
            code.append("                    for (Map<String, Object> row : candidates) {\n");
        } else {
            // Fallback to full scan
            code.append("                // Check all groups that match the such that condition for " + gvName + "\n");
            code.append("                for (Map<String, Object> row : mfStruct.values()) {\n");
        }

        // Generate aggregate validation for this condition
        String aggValidation = generateAggregateValidation(suchThatCond);
        if (!aggValidation.isEmpty()) {
            code.append("                        // Skip if required aggregates weren't computed\n");
            code.append("                        if (!(" + aggValidation + ")) continue;\n");
            code.append("                        \n");
        }

        // Generate condition check
        String condition = generateConditionCodeWithRow(suchThatCond, "rs" + scanNum);
        code.append("                        if (" + condition + ") {\n");

        // Update aggregates for this grouping variable
        for (AggregateFunction agg : query.fVectors) {
            if (agg.getGroupingVarName().equals(gvName)) {
                generateAggregateUpdate(agg, "rs" + scanNum);
            }
        }

        code.append("                        }\n");  // close if (condition)
        code.append("                    }\n");      // close for loop

        if (indexAttr != null) {
            code.append("                }\n");  // close if (candidates != null)
        }

        code.append("            }\n");          // close while loop
        code.append("            rs" + scanNum + ".close();\n");
        code.append("            stmt" + scanNum + ".close();\n\n");
    }

    /**
     * Find an attribute that can be used for index lookup
     * Returns the attribute name if found, null otherwise
     */
    private String findIndexableAttribute(ConditionExpression cond) {
        if (cond == null || cond.conditions.isEmpty()) {
            return null;
        }

        for (Condition c : cond.conditions) {
            // Look for equality conditions like "x.prod = prod"
            if (c.operator.equals("=") && !c.left.contains("(") && !c.right.contains("(")) {
                String leftAttr = c.left.contains(".") ? c.left.split("\\.")[1] : c.left;
                String rightAttr = c.right.contains(".") ? c.right.split("\\.")[1] : c.right;

                // Check if one side is a grouping attribute
                if (query.groupingAttributes.contains(leftAttr)) {
                    return leftAttr;
                }
                if (query.groupingAttributes.contains(rightAttr)) {
                    return rightAttr;
                }
            }
        }

        return null;
    }

    private String generateConditionCodeWithRow(ConditionExpression expr, String rsVar) {
        if (expr == null || expr.conditions.isEmpty()) {
            return "true";
        }

        StringBuilder cond = new StringBuilder();
        for (int i = 0; i < expr.conditions.size(); i++) {
            if (i > 0 && i - 1 < expr.operators.size()) {
                String op = expr.operators.get(i - 1);
                String javaOp = op.equalsIgnoreCase("and") ? "&&" : "||";
                cond.append(" ").append(javaOp).append(" ");
            }
            cond.append(generateSingleConditionWithRow(expr.conditions.get(i), rsVar));
        }
        return cond.toString();
    }

    private String generateSingleConditionWithRow(Condition cond, String rsVar) {
        String left = cond.left;
        String right = cond.right;
        String operator = cond.operator;

        // Check if this involves arithmetic expressions (which are always numeric)
        boolean hasArithmetic = containsArithmeticOp(left) || containsArithmeticOp(right);

        // Check if this is a numeric comparison
        boolean isNumericComparison = operator.equals("<") || operator.equals(">") ||
                                      operator.equals("<=") || operator.equals(">=") ||
                                      hasArithmetic;

        // Translate left side
        left = translateOperand(left, rsVar, isNumericComparison);

        // Translate right side
        right = translateOperand(right, rsVar, isNumericComparison);

        // Handle equality operators
        if (operator.equals("=")) {
            operator = isNumericComparison ? "==" : ".equals";
        } else if (operator.equals("<>") || operator.equals("!=")) {
            operator = isNumericComparison ? "!=" : ".notEquals";
        }

        // Generate the condition
        if (operator.equals(".equals")) {
            return left + ".equals(" + right + ")";
        } else if (operator.equals(".notEquals")) {
            return "!" + left + ".equals(" + right + ")";
        } else {
            return "(" + left + " " + operator + " " + right + ")";
        }
    }

    private boolean containsArithmeticOp(String expr) {
        // Check if expression contains arithmetic operators
        return expr.contains("+") || expr.contains("-") ||
               expr.contains("*") || expr.contains("/");
    }

    private String translateOperand(String operand, String rsVar, boolean isNumeric) {
        // Check for aggregate functions (e.g., avg(x.quant), sum(y.sale))
        if (operand.contains("(") && operand.contains(")")) {
            return translateAggregateReference(operand);
        }

        // Check for arithmetic expressions (e.g., x.month-1, y.month+1)
        String[] arithmeticOps = {"+", "-", "*", "/"};
        for (String op : arithmeticOps) {
            if (operand.contains(op)) {
                // Split by the operator but handle it carefully
                // For now, handle simple cases like "x.month-1"
                return translateArithmeticExpression(operand, rsVar);
            }
        }

        // Simple operand translation
        if (operand.startsWith("'")) {
            // String literal - convert single quotes to double quotes for Java
            return "\"" + operand.substring(1, operand.length() - 1) + "\"";
        } else if (operand.matches("\\d+")) {
            // Number literal - keep as is
            return operand;
        } else if (operand.contains(".")) {
            // Attribute with prefix (e.g., x.month)
            String attr = operand.split("\\.")[1];
            if (isNumeric) {
                return rsVar + ".getInt(\"" + attr + "\")";
            } else {
                return rsVar + ".getString(\"" + attr + "\")";
            }
        } else {
            // Check if this is a grouping attribute
            if (query.groupingAttributes.contains(operand)) {
                // Reference from mf-structure row
                if (isNumeric) {
                    return "Integer.parseInt(String.valueOf(row.get(\"" + operand + "\")))";
                } else {
                    return "String.valueOf(row.get(\"" + operand + "\"))";
                }
            } else {
                // Regular column reference
                if (isNumeric) {
                    return rsVar + ".getInt(\"" + operand + "\")";
                } else {
                    return rsVar + ".getString(\"" + operand + "\")";
                }
            }
        }
    }

    private String translateAggregateReference(String aggRef) {
        // Parse aggregate function like "avg(x.quant)" or "sum(y.sale)"
        int openParen = aggRef.indexOf("(");
        int closeParen = aggRef.lastIndexOf(")");

        if (openParen == -1 || closeParen == -1) {
            // Not actually an aggregate, treat as regular operand
            return aggRef;
        }

        String funcName = aggRef.substring(0, openParen).trim();
        String inside = aggRef.substring(openParen + 1, closeParen).trim();

        // inside should be like "x.quant"
        if (inside.contains(".")) {
            String[] parts = inside.split("\\.");
            String gv = parts[0].trim();
            String attr = parts[1].trim();

            // Find the matching aggregate in fVectors
            for (AggregateFunction agg : query.fVectors) {
                if (agg.getFunctionName().equals(funcName) &&
                    agg.getGroupingVarName().equals(gv) &&
                    agg.getAttribute().equals(attr)) {
                    String aggName = formatAggregateName(agg);

                    // Return appropriate cast based on function type
                    if (funcName.equals("avg")) {
                        return "((Double)row.get(\"" + aggName + "\"))";
                    } else {
                        return "((Integer)row.get(\"" + aggName + "\"))";
                    }
                }
            }

            // Aggregate should have been added by parser's extractAggregatesFromSuchThat
            // If not found, this is an error - but we'll handle gracefully
            throw new RuntimeException("Aggregate " + aggRef + " not found in fVectors. Parser should have extracted it.");
        }

        // Fallback
        return aggRef;
    }

    private String translateArithmeticExpression(String expr, String rsVar) {
        // Handle expressions like "x.month-1", "y.month+1", etc.
        // This is a simplified parser for basic arithmetic

        // Find the operator
        String[] ops = {"+", "-", "*", "/"};
        String op = null;
        int opIndex = -1;

        // Find the last operator (to handle precedence simply)
        for (String o : new String[]{"+", "-"}) {
            int idx = expr.lastIndexOf(o);
            // Make sure it's not at the start (negative number)
            if (idx > 0) {
                op = o;
                opIndex = idx;
                break;
            }
        }

        if (op == null) {
            // Try multiplication/division
            for (String o : new String[]{"*", "/"}) {
                int idx = expr.lastIndexOf(o);
                if (idx > 0) {
                    op = o;
                    opIndex = idx;
                    break;
                }
            }
        }

        if (op != null && opIndex > 0) {
            String leftPart = expr.substring(0, opIndex).trim();
            String rightPart = expr.substring(opIndex + 1).trim();

            // Recursively translate both parts
            String leftTranslated = translateOperand(leftPart, rsVar, true);
            String rightTranslated = translateOperand(rightPart, rsVar, true);

            return "(" + leftTranslated + " " + op + " " + rightTranslated + ")";
        }

        // If no operator found, treat as simple operand
        return translateOperand(expr, rsVar, true);
    }

    private void generateAggregateUpdate(AggregateFunction agg, String rsVar) {
        String aggName = formatAggregateName(agg);
        String attr = agg.getAttribute();

        code.append("                        // Update " + aggName + "\n");

        switch (agg.getFunctionName()) {
            case "count":
                code.append("                        row.put(\"" + aggName + "\", (Integer)row.get(\"" + aggName + "\") + 1);\n");
                break;

            case "sum":
                code.append("                        row.put(\"" + aggName + "\", (Integer)row.get(\"" + aggName + "\") + " + rsVar + ".getInt(\"" + attr + "\"));\n");
                break;

            case "max":
                code.append("                        row.put(\"" + aggName + "\", Math.max((Integer)row.get(\"" + aggName + "\"), " + rsVar + ".getInt(\"" + attr + "\")));\n");
                break;

            case "min":
                code.append("                        int current = (Integer)row.get(\"" + aggName + "\");\n");
                code.append("                        if (current == 0) current = Integer.MAX_VALUE;\n");
                code.append("                        row.put(\"" + aggName + "\", Math.min(current, " + rsVar + ".getInt(\"" + attr + "\")));\n");
                break;

            case "avg":
                code.append("                        // AVG requires count and sum\n");
                code.append("                        String countKey = \"" + aggName + "_count\";\n");
                code.append("                        String sumKey = \"" + aggName + "_sum\";\n");
                code.append("                        int cnt = row.containsKey(countKey) ? (Integer)row.get(countKey) : 0;\n");
                code.append("                        int sum = row.containsKey(sumKey) ? (Integer)row.get(sumKey) : 0;\n");
                code.append("                        cnt++;\n");
                code.append("                        sum += " + rsVar + ".getInt(\"" + attr + "\");\n");
                code.append("                        row.put(countKey, cnt);\n");
                code.append("                        row.put(sumKey, sum);\n");
                code.append("                        row.put(\"" + aggName + "\", (double)sum / cnt);\n");
                break;
        }
    }

    private void generateHavingAndOutput() {
        code.append("            // Apply HAVING clause and output results\n");
        code.append("            System.out.println(\"\\nQuery Results:\");\n");
        code.append("            System.out.println(\"" + String.join(" | ", query.selectAttributes) + "\");\n");
        code.append("            System.out.println(\"" + "-".repeat(60) + "\");\n");
        code.append("            \n");
        code.append("            for (Map<String, Object> row : mfStruct.values()) {\n");

        // Apply HAVING if present
        if (query.havingConditions != null && !query.havingConditions.conditions.isEmpty()) {
            String havingCode = generateHavingCondition(query.havingConditions);
            code.append("                if (" + havingCode + ") {\n");
            generateOutputRow("    ");
            code.append("                }\n");
        } else {
            // If no HAVING clause, add implicit filter to skip rows with all zero COUNT aggregates
            String zeroCountFilter = generateZeroCountFilter();
            if (!zeroCountFilter.isEmpty()) {
                code.append("                if (" + zeroCountFilter + ") {\n");
                generateOutputRow("    ");
                code.append("                }\n");
            } else {
                generateOutputRow("");
            }
        }

        code.append("            }\n\n");
        code.append("            conn.close();\n");
    }

    private void generateOutputRow(String indent) {
        code.append(indent + "                    System.out.print(");
        for (int i = 0; i < query.selectAttributes.size(); i++) {
            if (i > 0) code.append(" + \" | \" + ");

            String attr = query.selectAttributes.get(i);

            // Check if this is an expression or simple attribute
            if (attr.contains("(")) {
                // Check if it's an expression (contains operators)
                if (containsOperator(attr)) {
                    // Replace all aggregate functions in the expression
                    String expr = translateSelectExpression(attr);
                    code.append(expr);
                } else {
                    // Simple aggregate function
                    String key = attr;
                    for (AggregateFunction agg : query.fVectors) {
                        String aggStr = agg.getFunctionName() + "(" + agg.getGroupingVarName() + "." + agg.getAttribute() + ")";
                        if (attr.equals(aggStr)) {
                            key = formatAggregateName(agg);
                            break;
                        }
                    }
                    code.append("row.get(\"" + key + "\")");
                }
            } else {
                // Simple column reference
                code.append("row.get(\"" + attr + "\")");
            }
        }
        code.append(");\n");
        code.append(indent + "                    System.out.println();\n");
    }

    private boolean containsOperator(String expr) {
        return expr.contains("/") || expr.contains("*") ||
               expr.contains("+") || expr.contains("-");
    }

    private String translateSelectExpression(String expr) {
        String result = expr;

        // Check if expression contains division - if so, cast to double
        boolean hasDivision = expr.contains("/");

        // Replace all aggregate functions with row.get() calls
        for (AggregateFunction agg : query.fVectors) {
            String aggStr = agg.getFunctionName() + "(" + agg.getGroupingVarName() + "." + agg.getAttribute() + ")";
            String aggName = formatAggregateName(agg);

            // Cast to appropriate type
            String replacement;
            if (agg.getFunctionName().equals("avg")) {
                // AVG is already stored as double
                replacement = "((Double)row.get(\"" + aggName + "\"))";
            } else if (hasDivision) {
                // Cast integer aggregates to double for division
                replacement = "((Integer)row.get(\"" + aggName + "\")).doubleValue()";
            } else {
                replacement = "((Integer)row.get(\"" + aggName + "\"))";
            }

            result = result.replace(aggStr, replacement);
        }

        return result;
    }

    private String generateConditionCode(ConditionExpression expr, String rsVar) {
        if (expr == null || expr.conditions.isEmpty()) {
            return "true";
        }

        StringBuilder cond = new StringBuilder();
        for (int i = 0; i < expr.conditions.size(); i++) {
            if (i > 0 && i - 1 < expr.operators.size()) {
                String op = expr.operators.get(i - 1);
                String javaOp = op.equalsIgnoreCase("AND") ? "&&" : "||";
                cond.append(" ").append(javaOp).append(" ");
            }
            cond.append(generateSingleCondition(expr.conditions.get(i), rsVar));
        }
        return cond.toString();
    }

    private String generateSingleCondition(Condition cond, String rsVar) {
        String left = cond.left;
        String right = cond.right;

        // Replace attribute references with rs.getString()
        if (left.contains(".")) {
            String attr = left.split("\\.")[1];
            left = rsVar + ".getString(\"" + attr + "\")";
        } else if (!left.startsWith("'") && !left.matches("\\d+")) {
            left = rsVar + ".getString(\"" + left + "\")";
        }

        // Handle right side
        if (right.startsWith("'")) {
            // String literal - convert single quotes to double quotes for Java
            right = "\"" + right.substring(1, right.length() - 1) + "\"";
        } else if (right.matches("\\d+")) {
            // Number literal - keep as is
        } else if (!right.contains(".")) {
            right = rsVar + ".getString(\"" + right + "\")";
        }

        String operator = cond.operator;
        if (operator.equals("=")) operator = ".equals";

        if (operator.equals(".equals")) {
            return left + ".equals(" + right + ")";
        } else {
            return "(" + left + " " + operator + " " + right + ")";
        }
    }

    private String generateHavingCondition(ConditionExpression expr) {
        if (expr == null || expr.conditions.isEmpty()) {
            return "true";
        }

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < expr.conditions.size(); i++) {
            if (i > 0 && i - 1 < expr.operators.size()) {
                String op = expr.operators.get(i - 1);
                String javaOp = op.equalsIgnoreCase("and") ? "&&" : "||";
                result.append(" ").append(javaOp).append(" ");
            }
            result.append(generateHavingSingleCondition(expr.conditions.get(i)));
        }
        return result.toString();
    }

    private String generateHavingSingleCondition(Condition cond) {
        String left = translateHavingOperand(cond.left);
        String right = translateHavingOperand(cond.right);
        String operator = cond.operator;

        // Determine if we need to cast to double or int
        // For COUNT aggregates, use Integer comparison
        boolean isCountComparison = cond.left.contains("count") || cond.right.contains("count");

        if (isCountComparison) {
            // Cast to Integer for count comparisons
            String leftCast = isNumericLiteral(cond.left) ? left :
                             (left.startsWith("row.get") ? "((Integer)" + left + ")" : "((Integer)(" + left + "))");
            String rightCast = isNumericLiteral(cond.right) ? right :
                              (right.startsWith("row.get") ? "((Integer)" + right + ")" : "((Integer)(" + right + "))");

            if (operator.equals("=")) {
                return "(" + leftCast + " == " + rightCast + ")";
            } else {
                return "(" + leftCast + " " + operator + " " + rightCast + ")";
            }
        } else {
            // Cast to double for other comparisons (AVG, SUM, etc.)
            String leftCast = "((Double)" + left + ")";
            String rightCast = isNumericLiteral(cond.right) ? right : "((Double)" + right + ")";

            if (operator.equals("=")) {
                return "(" + leftCast + " == " + rightCast + ")";
            } else {
                return "(" + leftCast + " " + operator + " " + rightCast + ")";
            }
        }
    }

    private boolean isNumericLiteral(String s) {
        return s.matches("\\d+");
    }

    private String translateHavingOperand(String operand) {
        operand = operand.trim();

        // Check for arithmetic expressions (e.g., "count(x.prod) / 2")
        // Look for operators outside of parentheses
        int parenDepth = 0;
        for (int i = 0; i < operand.length(); i++) {
            char c = operand.charAt(i);
            if (c == '(') parenDepth++;
            else if (c == ')') parenDepth--;
            else if (parenDepth == 0 && (c == '+' || c == '-' || c == '*' || c == '/')) {
                // Found an operator at depth 0 - this is an arithmetic expression
                String left = operand.substring(0, i).trim();
                String right = operand.substring(i + 1).trim();
                String operator = String.valueOf(c);

                // Recursively translate both sides
                String leftCode = translateHavingOperand(left);
                String rightCode = translateHavingOperand(right);

                // Cast to appropriate type
                String leftCast = leftCode.startsWith("row.get") ? "((Integer)" + leftCode + ")" : leftCode;
                String rightCast = rightCode.startsWith("row.get") ? "((Integer)" + rightCode + ")" : rightCode;

                return "(" + leftCast + " " + operator + " " + rightCast + ")";
            }
        }

        // Check if it's an aggregate function like avg(x.sale) or count(x.prod)
        if (operand.contains("(") && operand.contains(")")) {
            // Extract function name and parameters
            int openParen = operand.indexOf("(");
            int closeParen = operand.lastIndexOf(")");
            String funcName = operand.substring(0, openParen).trim();
            String inside = operand.substring(openParen + 1, closeParen).trim();

            // inside is like "x.sale"
            String[] parts = inside.split("\\.");
            if (parts.length == 2) {
                String gv = parts[0].trim();
                String attr = parts[1].trim();

                // Find matching aggregate
                for (AggregateFunction agg : query.fVectors) {
                    if (agg.getFunctionName().equals(funcName) &&
                        agg.getGroupingVarName().equals(gv) &&
                        agg.getAttribute().equals(attr)) {
                        String aggName = formatAggregateName(agg);
                        return "row.get(\"" + aggName + "\")";
                    }
                }
            }
        }

        // Check if it's a number
        if (operand.matches("\\d+")) {
            return operand;
        }

        // Otherwise assume it's a column reference
        return "row.get(\"" + operand + "\")";
    }

    private String formatAggregateName(AggregateFunction agg) {
        return agg.getFunctionName() + "_" + agg.getGroupingVarName() + "_" + agg.getAttribute();
    }

    /**
     * Generate validation code to check if aggregates referenced in a condition were actually computed
     * Returns empty string if no validation needed, otherwise returns a boolean expression
     */
    private String generateAggregateValidation(ConditionExpression expr) {
        if (expr == null || expr.conditions.isEmpty()) {
            return "";
        }

        List<String> validations = new ArrayList<>();

        // Scan all conditions for aggregate references
        for (Condition cond : expr.conditions) {
            // Check left side
            List<AggregateFunction> leftAggs = findAggregateReferences(cond.left);
            for (AggregateFunction agg : leftAggs) {
                if (agg.getFunctionName().equals("avg")) {
                    String aggName = formatAggregateName(agg);
                    String countKey = aggName + "_count";
                    String validation = "row.containsKey(\"" + countKey + "\") && (Integer)row.get(\"" + countKey + "\") > 0";
                    if (!validations.contains(validation)) {
                        validations.add(validation);
                    }
                }
            }

            // Check right side
            List<AggregateFunction> rightAggs = findAggregateReferences(cond.right);
            for (AggregateFunction agg : rightAggs) {
                if (agg.getFunctionName().equals("avg")) {
                    String aggName = formatAggregateName(agg);
                    String countKey = aggName + "_count";
                    String validation = "row.containsKey(\"" + countKey + "\") && (Integer)row.get(\"" + countKey + "\") > 0";
                    if (!validations.contains(validation)) {
                        validations.add(validation);
                    }
                }
            }
        }

        if (validations.isEmpty()) {
            return "";
        }

        return String.join(" && ", validations);
    }

    /**
     * Find aggregate function references in an operand string
     */
    private List<AggregateFunction> findAggregateReferences(String operand) {
        List<AggregateFunction> result = new ArrayList<>();

        // Check if this looks like an aggregate function
        if (!operand.contains("(") || !operand.contains(")")) {
            return result;
        }

        // Parse it
        int openParen = operand.indexOf("(");
        int closeParen = operand.lastIndexOf(")");
        String funcName = operand.substring(0, openParen).trim();
        String inside = operand.substring(openParen + 1, closeParen).trim();

        if (inside.contains(".")) {
            String[] parts = inside.split("\\.");
            String gv = parts[0].trim();
            String attr = parts[1].trim();

            // Find matching aggregate in fVectors
            for (AggregateFunction agg : query.fVectors) {
                if (agg.getFunctionName().equals(funcName) &&
                    agg.getGroupingVarName().equals(gv) &&
                    agg.getAttribute().equals(attr)) {
                    result.add(agg);
                    break;
                }
            }
        }

        return result;
    }

    /**
     * Generate a filter to skip rows where all COUNT aggregates are zero
     * Returns empty string if no COUNT aggregates exist
     */
    private String generateZeroCountFilter() {
        List<String> countChecks = new ArrayList<>();

        // Find all COUNT aggregates
        for (AggregateFunction agg : query.fVectors) {
            if (agg.getFunctionName().equals("count")) {
                String aggName = formatAggregateName(agg);
                countChecks.add("(Integer)row.get(\"" + aggName + "\") > 0");
            }
        }

        if (countChecks.isEmpty()) {
            return "";
        }

        // Return true if ANY count is > 0 (using OR)
        return String.join(" || ", countChecks);
    }

    /**
     * OPTIMIZATION 2: Group grouping variables by dependency
     * Variables that don't depend on each other can be computed in the same scan
     */
    private List<List<String>> groupVariablesByDependency() {
        List<List<String>> groups = new ArrayList<>();
        Set<String> processed = new HashSet<>();

        // For now, simple implementation: check if variables are independent
        // Two variables are independent if neither uses the other's aggregates
        for (String gv1 : query.groupingVariableNames) {
            if (processed.contains(gv1)) continue;

            List<String> group = new ArrayList<>();
            group.add(gv1);
            processed.add(gv1);

            // Try to add other independent variables to this group
            for (String gv2 : query.groupingVariableNames) {
                if (processed.contains(gv2)) continue;

                // Check if gv2 is independent of all variables in the current group
                boolean independent = true;
                for (String gvInGroup : group) {
                    if (hasAggregateDepend(gv2, gvInGroup) || hasAggregateDepend(gvInGroup, gv2)) {
                        independent = false;
                        break;
                    }
                }

                if (independent) {
                    group.add(gv2);
                    processed.add(gv2);
                }
            }

            groups.add(group);
        }

        return groups;
    }

    /**
     * Check if gv1's conditions reference gv2's aggregates
     */
    private boolean hasAggregateDepend(String gv1, String gv2) {
        ConditionExpression cond = query.suchThatMap.get(gv1);
        if (cond == null) return false;

        // Check all conditions for aggregate references to gv2
        for (Condition c : cond.conditions) {
            if (referencesVariable(c.left, gv2) || referencesVariable(c.right, gv2)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Check if expression contains aggregate reference to given variable
     */
    private boolean referencesVariable(String expr, String gv) {
        // Look for patterns like "avg(gv.attr)" or "sum(gv.attr)"
        if (!expr.contains("(") || !expr.contains(")")) {
            return false;
        }

        int openParen = expr.indexOf("(");
        int closeParen = expr.lastIndexOf(")");
        String inside = expr.substring(openParen + 1, closeParen);

        if (inside.contains(".")) {
            String[] parts = inside.split("\\.");
            return parts[0].trim().equals(gv);
        }

        return false;
    }

    /**
     * OPTIMIZATION 2: Generate a combined scan that computes multiple grouping variables
     */
    private void generateCombinedScan(int scanNum, List<String> gvNames) {
        code.append("            // SCAN " + scanNum + ": Combined scan for variables: " + String.join(", ", gvNames) + "\n");
        code.append("            // OPTIMIZATION 2: Computing independent variables in single scan\n");
        code.append("            Statement stmt" + scanNum + " = conn.createStatement();\n");

        String selectQuery = buildSelectQuery();
        code.append("            ResultSet rs" + scanNum + " = stmt" + scanNum + ".executeQuery(\"" + selectQuery + "\");\n");
        code.append("            \n");
        code.append("            while (rs" + scanNum + ".next()) {\n");

        // Generate code for each grouping variable
        for (String gvName : gvNames) {
            ConditionExpression suchThatCond = query.suchThatMap.get(gvName);
            String indexAttr = findIndexableAttribute(suchThatCond);

            code.append("                // Process grouping variable " + gvName + "\n");

            if (indexAttr != null) {
                code.append("                String lookupVal_" + gvName + " = rs" + scanNum + ".getString(\"" + indexAttr + "\");\n");
                code.append("                List<Map<String, Object>> candidates_" + gvName + " = index_" + indexAttr + ".get(lookupVal_" + gvName + ");\n");
                code.append("                if (candidates_" + gvName + " != null) {\n");
                code.append("                    for (Map<String, Object> row : candidates_" + gvName + ") {\n");
            } else {
                code.append("                for (Map<String, Object> row : mfStruct.values()) {\n");
            }

            // Generate condition check
            String condition = generateConditionCodeWithRow(suchThatCond, "rs" + scanNum);
            code.append("                        if (" + condition + ") {\n");

            // Update aggregates for this grouping variable
            for (AggregateFunction agg : query.fVectors) {
                if (agg.getGroupingVarName().equals(gvName)) {
                    generateAggregateUpdate(agg, "rs" + scanNum);
                }
            }

            code.append("                        }\n");  // close if (condition)
            code.append("                    }\n");      // close for loop

            if (indexAttr != null) {
                code.append("                }\n");  // close if (candidates != null)
            }
        }

        code.append("            }\n");          // close while loop
        code.append("            rs" + scanNum + ".close();\n");
        code.append("            stmt" + scanNum + ".close();\n\n");
    }

    private void generateHelperMethods() {
        // Can add helper methods here if needed
    }

    /**
     * Write generated code to a file
     */
    public void writeToFile(String filename) throws IOException {
        try (FileWriter writer = new FileWriter(filename)) {
            writer.write(code.toString());
        }
    }
}