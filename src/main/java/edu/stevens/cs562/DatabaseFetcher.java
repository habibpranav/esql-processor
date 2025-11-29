package edu.stevens.cs562;

import java.sql.*;
import java.util.ArrayList;

public class DatabaseFetcher {

    public static ArrayList<Row> fetchAllRows(Connection conn, String tableName) throws Exception {

        ArrayList<Row> rows = new ArrayList<>();

        // Build dynamic query
        String sql = "SELECT * FROM " + tableName;

        PreparedStatement stmt = conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();

        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        while (rs.next()) {
            Row row = new Row();

            for (int i = 1; i <= columnCount; i++) {
                String colName = meta.getColumnName(i);
                Object value = rs.getObject(i);

                row.put(colName, value);
            }

            rows.add(row);
        }

        return rows;
    }
}