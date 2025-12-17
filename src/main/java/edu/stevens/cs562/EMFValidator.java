package edu.stevens.cs562;

public class EMFValidator {
    public void validate(EMFQuery query) {
        // FROM is required
        if (query.fromTable == null || query.fromTable.isEmpty()) {
            throw new RuntimeException("FROM clause is required");
        }

        // SELECT attributes are required
        if (query.selectAttributes == null || query.selectAttributes.isEmpty()) {
            throw new RuntimeException("SELECT attributes are required");
        }

        // GROUP BY attributes are required
        if (query.groupingAttributes == null || query.groupingAttributes.isEmpty()) {
            throw new RuntimeException("GROUP BY attributes are required");
        }

        // Grouping variables are required
        if (query.groupingVariableNames == null || query.groupingVariableNames.isEmpty()) {
            throw new RuntimeException("Grouping variables are required");
        }

        // F-VECTORS (aggregates) are required
        if (query.fVectors == null || query.fVectors.isEmpty()) {
            throw new RuntimeException("Aggregate functions (F-VECT) are required");
        }

        // SUCH THAT conditions are required
        if (query.suchThatMap == null || query.suchThatMap.isEmpty()) {
            throw new RuntimeException("SUCH THAT conditions are required");
        }
    }
}