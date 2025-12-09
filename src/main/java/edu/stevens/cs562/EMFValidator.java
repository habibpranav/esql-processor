package edu.stevens.cs562;

public class EMFValidator {
    public void validate(EMFQuery query) {
        // Basic validation
        if (query.fromTable == null || query.fromTable.isEmpty()) {
            throw new RuntimeException("FROM clause is required");
        }
        if (query.groupingAttributes == null || query.groupingAttributes.isEmpty()) {
            throw new RuntimeException("GROUP BY clause is required");
        }
    }
}