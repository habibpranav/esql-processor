package edu.stevens.cs562;

import java.util.ArrayList;
import java.util.HashMap;

public class Row {

    private HashMap<String, Object> values;

    public Row() {
        this.values = new HashMap<>();
    }

    // Store a column value
    public void put(String columnName, Object value) {
        values.put(columnName, value);
    }

    // Retrieve a column value
    public Object get(String columnName) {
        return values.get(columnName);
    }

    // Retrieve all column names
    public ArrayList<String> getColumnNames() {
        return new ArrayList<>(values.keySet());
    }

    // Return internal HashMap directly
    public HashMap<String, Object> getValues() {
        return values;
    }

    @Override // For debugging
    public String toString() {
        return values.toString();
    }
}