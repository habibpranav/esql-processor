
DROP VIEW IF EXISTS Z_view;
DROP VIEW IF EXISTS X_view;
DROP VIEW IF EXISTS Y_view;

-----------------------------------------
-- Z: average for current month
-----------------------------------------
CREATE VIEW Z_view AS
SELECT cust,
       month,
       AVG(quant) AS avg_quant
FROM sales
GROUP BY cust, month;

-----------------------------------------
-- X: average for all previous months
-----------------------------------------
CREATE VIEW X_view AS
SELECT s1.cust,
       s1.month,
       AVG(s2.quant) AS avg_quant
FROM (SELECT DISTINCT cust, month FROM sales) AS s1
         LEFT JOIN sales AS s2
                   ON s1.cust = s2.cust
                       AND s2.month < s1.month
GROUP BY s1.cust, s1.month;


-- Y: average for all future months
CREATE VIEW Y_view AS
SELECT s1.cust,
       s1.month,
       AVG(s2.quant) AS avg_quant
FROM (SELECT DISTINCT cust, month FROM sales) AS s1
         LEFT JOIN sales AS s2
                   ON s1.cust = s2.cust
                       AND s2.month > s1.month
GROUP BY s1.cust, s1.month;


SELECT
    Z.cust,
    Z.month,
    X.avg_quant AS avg_before,
    Z.avg_quant AS avg_current,
    Y.avg_quant AS avg_after
FROM Z_view AS Z
         LEFT JOIN X_view AS X
                   ON Z.cust = X.cust AND Z.month = X.month
         LEFT JOIN Y_view AS Y
                   ON Z.cust = Y.cust AND Z.month = Y.month;