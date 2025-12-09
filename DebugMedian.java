import java.sql.*;
import java.util.*;
import java.io.*;

public class DebugMedian {
    public static void main(String[] args) {
        try {
            Properties dbProps = new Properties();
            try (InputStream input = new FileInputStream("src/main/resources/db.properties")) {
                dbProps.load(input);
            }
            
            String dbName = dbProps.getProperty("db.name");
            String user = dbProps.getProperty("db.user");
            String password = dbProps.getProperty("db.password");
            String url = "jdbc:postgresql://localhost:5432/" + dbName;
            Connection conn = DriverManager.getConnection(url, user, password);

            Map<String, Map<String, Object>> mfStruct = new HashMap<>();

            Statement stmt0 = conn.createStatement();
            ResultSet rs0 = stmt0.executeQuery("select * from sales where year=2016");
            
            while (rs0.next()) {
                String key = rs0.getString("prod") + "|" + rs0.getString("quant");
                
                if (!mfStruct.containsKey(key)) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("prod", rs0.getString("prod"));
                    row.put("quant", rs0.getString("quant"));
                    row.put("count_x_prod", 0);
                    row.put("count_y_prod", 0);
                    mfStruct.put(key, row);
                }
            }
            rs0.close();
            stmt0.close();

            System.out.println("MF-Structure has " + mfStruct.size() + " groups");

            // SCAN 1: count_x_prod (all sales for this product)
            Statement stmt1 = conn.createStatement();
            ResultSet rs1 = stmt1.executeQuery("select * from sales where year=2016");
            
            while (rs1.next()) {
                for (Map<String, Object> row : mfStruct.values()) {
                    if (rs1.getString("prod").equals(String.valueOf(row.get("prod")))) {
                        row.put("count_x_prod", (Integer)row.get("count_x_prod") + 1);
                    }
                }
            }
            rs1.close();
            stmt1.close();

            // SCAN 2: count_y_prod (sales with quant < current quant)
            Statement stmt2 = conn.createStatement();
            ResultSet rs2 = stmt2.executeQuery("select * from sales where year=2016");
            
            while (rs2.next()) {
                for (Map<String, Object> row : mfStruct.values()) {
                    if (rs2.getString("prod").equals(String.valueOf(row.get("prod"))) && 
                        (rs2.getInt("quant") < Integer.parseInt(String.valueOf(row.get("quant"))))) {
                        row.put("count_y_prod", (Integer)row.get("count_y_prod") + 1);
                    }
                }
            }
            rs2.close();
            stmt2.close();

            // Print H table for Grapes
            System.out.println("\nH-Table for Grapes:");
            System.out.printf("%-10s | %6s | %10s | %10s | %s%n", "prod", "quant", "count_x", "count_y", "count_x/2");
            System.out.println("------------------------------------------------------------------");
            
            List<Map<String, Object>> grapes = new ArrayList<>();
            for (Map<String, Object> row : mfStruct.values()) {
                if ("Grapes".equals(row.get("prod"))) {
                    grapes.add(row);
                }
            }
            
            // Sort by quant
            grapes.sort((a,b) -> Integer.compare(
                Integer.parseInt(String.valueOf(a.get("quant"))),
                Integer.parseInt(String.valueOf(b.get("quant")))
            ));
            
            for (Map<String, Object> row : grapes) {
                int countX = (Integer)row.get("count_x_prod");
                int countY = (Integer)row.get("count_y_prod");
                boolean matches = (countY == countX / 2);
                System.out.printf("%-10s | %6s | %10d | %10d | %d %s%n", 
                    row.get("prod"), row.get("quant"), countX, countY, countX/2, matches ? "<-- MATCH!" : "");
            }

            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
