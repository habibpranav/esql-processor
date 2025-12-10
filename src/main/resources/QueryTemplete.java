import java.sql.*;
import java.util.*;
import java.io.*;

/*
 * NOTE: This file is ONLY for editing convenience and IDE support.
 * The actual QueryGenerator uses QueryTemplate.txt (NOT this .java file) for code generation
 * because String.format() with %s placeholders is simpler and cleaner than parsing a Java file.
 * This .java file is just a mirror of QueryTemplate.txt to help with editing.
 * DO NOT pass this file to QueryGenerator - it reads QueryTemplate.txt directly
 */



public class GeneratedQuery {

    static List<Map<String, Object>> mfStruct = new ArrayList<>();
    static int NUM_OF_ENTRIES = 0;

%s

%s

%s

%s

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream("src/main/resources/db.properties"));
        String url = "jdbc:postgresql://localhost:5432/" + props.getProperty("db.name");
        Connection conn = DriverManager.getConnection(url,
                props.getProperty("db.user"),
                props.getProperty("db.password"));

%s

                %s

                %s

        conn.close();
    }

    static void updateAgg(Map<String, Object> row, String name, String func, int val) {
        if (func.equals("count")) {
            row.put(name, (Integer)row.get(name) + 1);
        } else if (func.equals("sum")) {
            row.put(name, (Integer)row.get(name) + val);
        } else if (func.equals("max")) {
            row.put(name, Math.max((Integer)row.get(name), val));
        } else if (func.equals("min")) {
            int cur = (Integer)row.get(name);
            if (cur == Integer.MAX_VALUE) cur = val;
            row.put(name, Math.min(cur, val));
        } else if (func.equals("avg")) {
            String cntKey = name + "_cnt";
            String sumKey = name + "_sum";
            int cnt = (Integer)row.getOrDefault(cntKey, 0) + 1;
            int sum = (Integer)row.getOrDefault(sumKey, 0) + val;
            row.put(cntKey, cnt);
            row.put(sumKey, sum);
            row.put(name, (double)sum / cnt);
        }
    }
}