package edu.stevens.cs562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;





/**
 * Holds EVERY part of the parsed ESQL/EMF query.
 *
 * SELECT
 * FROM
 * WHERE
 * GROUP BY
 * GROUPING ATTRIBUTES
 * SUCH THAT
 * HAVING
 *
 * This is what is passed into EMFEngine.
 */
public class EMFQuery {


    // Base table (FROM Sales)

    public String fromTable;


    // SELECT attributes (strings)

    public List<String> selectAttributes;


    // WHERE condition (before grouping)

    public ConditionExpression whereConditions;


    // GROUP BY attributes Example: "cust", "prod"

    public List<String> groupingAttributes;


    // GROUPING VARIABLE NAMES (X, Y, Z....n )

    public List<String> groupingVariableNames;

    // ----------------------------
    // SUCH THAT conditions for each grouping variable
    // Map: variable name -> condition expression
    // Example: "X" -> "X.cust = cust AND X.state = 'NY'"
    // ----------------------------
    public Map<String, ConditionExpression> suchThatMap;

    // ----------------------------
    // F-VECTORS (SUM_X_sale, AVG_Y_sale, etc.)
    // ----------------------------
    public List<AggregateFunction> fVectors;

    // ----------------------------
    // HAVING condition
    // ----------------------------
    public ConditionExpression havingConditions;

    public EMFQuery() {
        this.selectAttributes = new ArrayList<>();
        this.groupingAttributes = new ArrayList<>();
        this.groupingVariableNames = new ArrayList<>();
        this.suchThatMap = new HashMap<>();
        this.fVectors = new ArrayList<>();
        this.whereConditions = new ConditionExpression();
        this.havingConditions = new ConditionExpression();
    }



    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("+----------------------------------------------------------------------------+\n");
        sb.append("|              PARSED EMF QUERY STRUCTURE                                      |\n");
        sb.append("+-----------------------------------------------------------------------------+\n");

        sb.append(" SELECT:           ").append(selectAttributes).append("\n");
        sb.append(" FROM:             ").append(fromTable).append("\n");

        if (whereConditions != null && !whereConditions.conditions.isEmpty()) {
            sb.append(" WHERE:            ").append(whereConditions).append("\n");
        }

        sb.append(" GROUP BY:         ").append(groupingAttributes).append("\n");
        sb.append(" Grouping Vars:    ").append(groupingVariableNames).append("\n");

        if (suchThatMap != null && !suchThatMap.isEmpty()) {
            sb.append(" SUCH THAT:\n");
            for (Map.Entry<String, ConditionExpression> entry : suchThatMap.entrySet()) {
                sb.append("   ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
        }

        if (fVectors != null && !fVectors.isEmpty()) {
            sb.append(" F-VECTORS:        ").append(fVectors).append("\n");
        }

        if (havingConditions != null && !havingConditions.conditions.isEmpty()) {
            sb.append(" HAVING:           ").append(havingConditions).append("\n");
        }

        sb.append("+--------------------------------------------------------------------------------+\n");

        return sb.toString();
    }





}