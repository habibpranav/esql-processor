package edu.stevens.cs562;

import java.io.*;
import java.nio.file.*;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        try {
            String rawQuery;
            int inputFormat = 1; // Default to ESQL

            if (args.length > 0) {
                // Read from file if argument provided
                rawQuery = readFile(args[0]);

                // Check if second argument specifies format
                if (args.length > 1) {
                    inputFormat = Integer.parseInt(args[1]);
                }
            } else {
                // Ask user for input format
                Scanner scanner = new Scanner(System.in);
                System.out.println("Select input format:");
                System.out.println("1. ESQL syntax");
                System.out.println("2. Phi operator format (interactive prompts)");
                System.out.print("Enter choice (1 or 2): ");
                inputFormat = scanner.nextInt();
                scanner.nextLine(); // consume newline

                // Read from stdin
                if (inputFormat == 1) {
                    System.out.println("\nEnter your EMF query in ESQL syntax (type 'DONE' on a new line when finished):");
                    StringBuilder queryBuilder = new StringBuilder();
                    while (scanner.hasNextLine()) {
                        String line = scanner.nextLine();
                        if (line.trim().equalsIgnoreCase("DONE")) {
                            break;
                        }
                        queryBuilder.append(line).append("\n");
                    }
                    rawQuery = queryBuilder.toString();
                } else {
                    // Interactive Phi operator prompts
                    System.out.println("\n=== PHI OPERATOR INPUT (6 OPERANDS) ===\n");

                    System.out.print("FROM (table name): ");
                    String fromTable = scanner.nextLine();

                    System.out.print("WHERE (σ0 condition, press Enter to skip): ");
                    String whereCondition = scanner.nextLine();

                    System.out.print("1. S (Select attributes): ");
                    String selectAttrs = scanner.nextLine();

                    System.out.print("2. n (Number of grouping variables): ");
                    int n = scanner.nextInt();
                    scanner.nextLine(); // consume newline

                    System.out.print("3. V (Grouping attributes): ");
                    String groupingAttrs = scanner.nextLine();

                    System.out.print("4. F-VECT (Aggregate functions, comma-separated): ");
                    String fVect = scanner.nextLine();

                    System.out.println("5. σ (Predicates for each grouping variable):");
                    StringBuilder predicates = new StringBuilder();
                    for (int i = 1; i <= n; i++) {
                        System.out.print("   σ" + i + " (Predicate for variable " + i + "): ");
                        predicates.append(scanner.nextLine()).append("\n");
                    }

                    System.out.print("6. G (HAVING condition, press Enter to skip): ");
                    String having = scanner.nextLine();

                    // Build Phi format query
                    StringBuilder queryBuilder = new StringBuilder();
                    queryBuilder.append("FROM:\n").append(fromTable).append("\n");
                    if (whereCondition != null && !whereCondition.trim().isEmpty()) {
                        queryBuilder.append("WHERE:\n").append(whereCondition).append("\n");
                    }
                    queryBuilder.append("SELECT ATTRIBUTE(S):\n").append(selectAttrs).append("\n");
                    queryBuilder.append("NUMBER OF GROUPING VARIABLES(n):\n").append(n).append("\n");
                    queryBuilder.append("GROUPING ATTRIBUTES(V):\n").append(groupingAttrs).append("\n");
                    queryBuilder.append("F-VECT([F]):\n").append(fVect).append("\n");
                    queryBuilder.append("SELECT CONDITION-VECT([σ]):\n").append(predicates.toString());
                    if (having != null && !having.trim().isEmpty()) {
                        queryBuilder.append("HAVING_CONDITION(G):\n").append(having).append("\n");
                    }
                    rawQuery = queryBuilder.toString();
                }
                scanner.close();
            }

            System.out.println("\n=== PARSING QUERY ===");
            System.out.println(rawQuery);
            System.out.println();

            EMFQuery emfQuery;
            if (inputFormat == 1) {
                // Parse ESQL syntax
                EMFParser parser = new EMFParser();
                emfQuery = parser.parse(rawQuery);
            } else {
                // Parse Phi operator format
                PhiInputParser phiParser = new PhiInputParser();
                emfQuery = phiParser.parse(rawQuery);
            }

            System.out.println("=== PARSED EMF QUERY ===");
            System.out.println(emfQuery);
            System.out.println();

            EMFValidator validator = new EMFValidator();
            validator.validate(emfQuery);
            System.out.println("Query validation passed");
            System.out.println();

            PhiConverter phiConverter = new PhiConverter();
            PhiOperator phi = phiConverter.convert(emfQuery);

            System.out.println("=== CONVERTING TO PHI OPERATOR ===");
            System.out.println(phi);

            System.out.println("=== GENERATING CODE ===");
            QueryGenerator generator = new QueryGenerator(phi);
            String generatedCode = generator.generate();

            String outputPath = "src/main/java/GeneratedQuery.java";
            Files.writeString(Paths.get(outputPath), generatedCode);
            System.out.println("Code generated: " + outputPath);

            System.out.println("\n=== GENERATED CODE ===");
            System.out.println(generatedCode);

            System.out.println("\n\n=== INSTRUCTIONS ===");
            System.out.println("To execute the generated query:");
            System.out.println("1. Compile: javac src/main/java/GeneratedQuery.java");
            System.out.println("2. Run: java -cp src/main/java GeneratedQuery");

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static String readFile(String filePath) throws IOException {
        return Files.readString(Paths.get(filePath));
    }
}