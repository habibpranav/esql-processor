DROP VIEW IF EXISTS X_view;
DROP VIEW IF EXISTS Y_view;

-- X: previous month average
CREATE VIEW X_view AS
SELECT prod, month + 1 AS month, AVG(quant) AS avg_quant
FROM sales
WHERE year = 2016
GROUP BY prod, month;

-- Y: next month average
CREATE VIEW Y_view AS
SELECT prod, month - 1 AS month, AVG(quant) AS avg_quant
FROM sales
WHERE year = 2016
GROUP BY prod, month;

-- Z: count of tuples where quant > avg(previous month)
--                            and quant < avg(next month)
SELECT
    s.prod,
    s.month,
    COUNT(*) AS count_z
FROM sales s
         JOIN X_view X ON s.prod = X.prod AND s.month = X.month
         JOIN Y_view Y ON s.prod = Y.prod AND s.month = Y.month
WHERE s.year = 2016
  AND s.quant > X.avg_quant
  AND s.quant < Y.avg_quant
GROUP BY s.prod, s.month;