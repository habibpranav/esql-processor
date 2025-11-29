package edu.stevens.cs562;

import java.util.ArrayList;
import java.util.Objects;

/**
 * Key represents one unique group in the GROUP BY operation.
 *
 * This Key object is used inside:
 *     HashMap<Key, ArrayList<Row>>
 *
 * Two Key objects are considered equal if their "values" list is equal.
 * The HashMap relies on equals() + hashCode() to group rows correctly.
 */
public class Key {

    private ArrayList<Object> values;   // ordered list of grouping attribute values

    public Key(ArrayList<Object> values) {
        this.values = values;
    }

    public ArrayList<Object> getValues() {
        return values;
    }

    @Override
    public boolean equals(Object o) {

        // Same object reference → equal
        if (this == o) return true;

        // Not the same type which means  not equal
        if (!(o instanceof Key)) return false;

        Key other = (Key) o;

        // Compare the ArrayLists
        return Objects.equals(this.values, other.values);
    }

    @Override
    public int hashCode() {

        // ArrayList already provides a stable hash based on order + contents
        return values.hashCode();
    }

    @Override
    public String toString() {
        return values.toString();
    }
}