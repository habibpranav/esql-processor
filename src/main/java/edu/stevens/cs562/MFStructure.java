package edu.stevens.cs562;

import java.util.*;

/**
 * THE CRITICAL DATA STRUCTURE - mf-structure
 *
 * This is the ONLY thing that should be kept in memory!
 * DO NOT load the entire sales table into memory.
 *
 * The mf-structure contains:
 * - One row per unique combination of grouping attributes
 * - Columns for all aggregate functions across all grouping variables
 *
 * Example: For grouping by "cust", we have one row per customer with:
 *   {cust: "Alice", count_1_quant: 5, sum_2_quant: 100, max_3_quant: 50}
 */
public class MFStructure {

    // Grouping attributes (e.g., ["cust"], or ["cust", "prod"])
    private List<String> groupingAttributes;

    // List of rows in the mf-structure
    // Each row is a Map: attribute_name -> value
    private List<Map<String, Object>> rows;

    // Index to quickly find a row by grouping key
    // Key: concatenated grouping attribute values (e.g., "Alice" or "Alice|ProductA")
    // Value: index in the rows list
    private Map<String, Integer> index;

    // Schema: maps column name -> data type
    private Map<String, Class<?>> schema;

    public MFStructure(List<String> groupingAttributes) {
        this.groupingAttributes = groupingAttributes;
        this.rows = new ArrayList<>();
        this.index = new HashMap<>();
        this.schema = new HashMap<>();

        // Add grouping attributes to schema
        for (String attr : groupingAttributes) {
            schema.put(attr, String.class); // Assuming String for now
        }
    }

    /**
     * Add an aggregate column to the schema
     */
    public void addAggregateColumn(String columnName, Class<?> type) {
        schema.put(columnName, type);
    }

    /**
     * Find or create a row for the given grouping attribute values
     * @param groupingValues Map of grouping attribute -> value
     * @return the row (Map)
     */
    public Map<String, Object> getOrCreateRow(Map<String, Object> groupingValues) {
        String key = createKey(groupingValues);

        if (index.containsKey(key)) {
            int rowIndex = index.get(key);
            return rows.get(rowIndex);
        }

        // Create new row
        Map<String, Object> newRow = new HashMap<>();

        // Set grouping attribute values
        for (String attr : groupingAttributes) {
            newRow.put(attr, groupingValues.get(attr));
        }

        // Initialize all aggregate columns with default values
        for (Map.Entry<String, Class<?>> entry : schema.entrySet()) {
            if (!groupingAttributes.contains(entry.getKey())) {
                newRow.put(entry.getKey(), getDefaultValue(entry.getValue()));
            }
        }

        // Add to rows and index
        index.put(key, rows.size());
        rows.add(newRow);

        return newRow;
    }

    /**
     * Create a unique key from grouping attribute values
     */
    private String createKey(Map<String, Object> groupingValues) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < groupingAttributes.size(); i++) {
            if (i > 0) sb.append("|");
            sb.append(groupingValues.get(groupingAttributes.get(i)));
        }
        return sb.toString();
    }

    /**
     * Get default value for a data type
     */
    private Object getDefaultValue(Class<?> type) {
        if (type == Integer.class) return 0;
        if (type == Double.class) return 0.0;
        if (type == Long.class) return 0L;
        if (type == String.class) return "";
        return null;
    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }

    public int size() {
        return rows.size();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MF-Structure (" + rows.size() + " rows)\n");
        sb.append("Grouping Attributes: ").append(groupingAttributes).append("\n");
        sb.append("Schema: ").append(schema.keySet()).append("\n\n");

        for (Map<String, Object> row : rows) {
            sb.append(row).append("\n");
        }

        return sb.toString();
    }
}
