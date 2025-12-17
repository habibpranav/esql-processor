package edu.stevens.cs562;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

/**
 * Unit tests for EMFParser to verify queries are parsed correctly.
 */
public class EMFParserTest {

    private EMFParser parser = new EMFParser();

    // ==================== Basic Query Parsing ====================

    @Test
    public void testSimpleQuery() {
        String query = """
            SELECT cust, sum(x.quant)
            FROM sales
            GROUP BY cust; x
            SUCH THAT x.state = 'NY'
            """;

        EMFQuery result = parser.parse(query);

        assertEquals("sales", result.fromTable);
        assertEquals(List.of("cust", "sum(x.quant)"), result.selectAttributes);
        assertEquals(List.of("cust"), result.groupingAttributes);
        assertEquals(List.of("x"), result.groupingVariableNames);
    }

    @Test
    public void testMultipleGroupingVariables() {
        String query = """
            SELECT cust, count(ny.quant), sum(nj.quant), max(ct.quant)
            FROM sales
            GROUP BY cust; ny, nj, ct
            SUCH THAT ny.cust = cust AND ny.state = 'NY',
                      nj.cust = cust AND nj.state = 'NJ',
                      ct.cust = cust AND ct.state = 'CT'
            """;

        EMFQuery result = parser.parse(query);

        assertEquals("sales", result.fromTable);
        assertEquals(List.of("cust"), result.groupingAttributes);
        assertEquals(List.of("ny", "nj", "ct"), result.groupingVariableNames);

        // Check SUCH THAT conditions were parsed
        assertTrue(result.suchThatMap.containsKey("ny"));
        assertTrue(result.suchThatMap.containsKey("nj"));
        assertTrue(result.suchThatMap.containsKey("ct"));
    }

    @Test
    public void testWithWhereClause() {
        String query = """
            SELECT cust, avg(x.quant)
            FROM sales
            WHERE year = 2020
            GROUP BY cust; x
            SUCH THAT x.state = 'NY'
            """;

        EMFQuery result = parser.parse(query);

        assertNotNull(result.whereConditions);
        assertFalse(result.whereConditions.conditions.isEmpty());
        assertEquals("year", result.whereConditions.conditions.get(0).left);
        assertEquals("2020", result.whereConditions.conditions.get(0).right);
    }

    @Test
    public void testWithHavingClause() {
        String query = """
            SELECT cust, sum(x.quant), sum(y.quant)
            FROM sales
            GROUP BY cust; x, y
            SUCH THAT x.state = 'NY', y.state = 'NJ'
            HAVING sum(x.quant) > sum(y.quant)
            """;

        EMFQuery result = parser.parse(query);

        assertNotNull(result.havingConditions);
        assertFalse(result.havingConditions.conditions.isEmpty());
    }

    // ==================== F-VECT Parsing ====================

    @Test
    public void testFVectorsExtracted() {
        String query = """
            SELECT cust, count(x.quant), sum(y.quant), avg(z.quant)
            FROM sales
            GROUP BY cust; x, y, z
            SUCH THAT x.state = 'NY', y.state = 'NJ', z.state = 'CT'
            """;

        EMFQuery result = parser.parse(query);

        assertEquals(3, result.fVectors.size());

        // Check first aggregate
        AggregateFunction agg1 = result.fVectors.get(0);
        assertEquals("count", agg1.getFunctionName());
        assertEquals("x", agg1.getGroupingVarName());
        assertEquals("quant", agg1.getAttribute());
    }

    @Test
    public void testAllAggregateFunctions() {
        String query = """
            SELECT cust, sum(a.quant), avg(b.quant), count(c.quant), min(d.quant), max(e.quant)
            FROM sales
            GROUP BY cust; a, b, c, d, e
            SUCH THAT a.state='NY', b.state='NJ', c.state='CT', d.state='PA', e.state='MA'
            """;

        EMFQuery result = parser.parse(query);

        assertEquals(5, result.fVectors.size());

        List<String> functions = result.fVectors.stream()
            .map(AggregateFunction::getFunctionName)
            .toList();

        assertTrue(functions.contains("sum"));
        assertTrue(functions.contains("avg"));
        assertTrue(functions.contains("count"));
        assertTrue(functions.contains("min"));
        assertTrue(functions.contains("max"));
    }

    // ==================== Condition Parsing ====================

    @Test
    public void testAndCondition() {
        String query = """
            SELECT cust, sum(x.quant)
            FROM sales
            GROUP BY cust; x
            SUCH THAT x.cust = cust AND x.state = 'NY'
            """;

        EMFQuery result = parser.parse(query);

        ConditionExpression expr = result.suchThatMap.get("x");
        assertEquals(2, expr.conditions.size());
        assertEquals(1, expr.operators.size());
        assertEquals("and", expr.operators.get(0));
    }

    @Test
    public void testOrCondition() {
        String query = """
            SELECT cust, sum(x.quant)
            FROM sales
            GROUP BY cust; x
            SUCH THAT x.state = 'NY' OR x.state = 'NJ'
            """;

        EMFQuery result = parser.parse(query);

        ConditionExpression expr = result.suchThatMap.get("x");
        assertEquals(2, expr.conditions.size());
        assertEquals("or", expr.operators.get(0));
    }

    @Test
    public void testComparisonOperators() {
        String query = """
            SELECT cust, sum(x.quant)
            FROM sales
            WHERE year >= 2020 AND month <= 6
            GROUP BY cust; x
            SUCH THAT x.quant > 100
            """;

        EMFQuery result = parser.parse(query);

        // Check WHERE conditions
        assertEquals(">=", result.whereConditions.conditions.get(0).operator);
        assertEquals("<=", result.whereConditions.conditions.get(1).operator);

        // Check SUCH THAT condition
        assertEquals(">", result.suchThatMap.get("x").conditions.get(0).operator);
    }

    // ==================== Case Insensitivity ====================

    @Test
    public void testCaseInsensitiveKeywords() {
        String query = """
            select cust, SUM(x.quant)
            FROM Sales
            GROUP BY cust; x
            such that x.state = 'NY'
            """;

        EMFQuery result = parser.parse(query);

        assertEquals("sales", result.fromTable);
        assertEquals(List.of("cust", "sum(x.quant)"), result.selectAttributes);
    }

    @Test
    public void testStringLiteralsCasePreserved() {
        String query = """
            SELECT cust, sum(x.quant)
            FROM sales
            GROUP BY cust; x
            SUCH THAT x.state = 'NY'
            """;

        EMFQuery result = parser.parse(query);

        ConditionExpression expr = result.suchThatMap.get("x");
        // 'NY' should remain uppercase, not become 'ny'
        assertEquals("'NY'", expr.conditions.get(0).right);
    }

    // ==================== Multiple Grouping Attributes ====================

    @Test
    public void testMultipleGroupingAttributes() {
        String query = """
            SELECT cust, prod, sum(x.quant)
            FROM sales
            GROUP BY cust, prod; x
            SUCH THAT x.state = 'NY'
            """;

        EMFQuery result = parser.parse(query);

        assertEquals(List.of("cust", "prod"), result.groupingAttributes);
    }

    // ==================== Error Cases ====================

    @Test
    public void testEmptyQueryThrowsException() {
        assertThrows(RuntimeException.class, () -> {
            parser.parse("");
        });
    }

    @Test
    public void testNullQueryThrowsException() {
        assertThrows(RuntimeException.class, () -> {
            parser.parse(null);
        });
    }

    @Test
    public void testMissingFromClause() {
        String query = """
            SELECT cust, sum(x.quant)
            GROUP BY cust; x
            SUCH THAT x.state = 'NY'
            """;

        assertThrows(RuntimeException.class, () -> {
            parser.parse(query);
        });
    }
}