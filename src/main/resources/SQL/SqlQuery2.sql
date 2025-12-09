DROP VIEW IF EXISTS X_view;
DROP VIEW IF EXISTS Y_view;

CREATE VIEW X_view AS
SELECT prod, month, SUM(quant) AS sum_quant
FROM sales
WHERE year = 2016
GROUP BY prod, month;

CREATE VIEW Y_view AS
SELECT prod, SUM(quant) AS sum_quant
FROM sales
WHERE year = 2016
GROUP BY prod;

SELECT
    X.prod,
    X.month,
    X.sum_quant::numeric / Y.sum_quant AS ratio
FROM X_view X
         JOIN Y_view Y ON X.prod = Y.prod;