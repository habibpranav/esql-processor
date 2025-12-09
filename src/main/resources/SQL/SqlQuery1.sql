DROP VIEW IF EXISTS X_view;
DROP VIEW IF EXISTS Y_view;
DROP VIEW IF EXISTS Z_view;

DROP VIEW IF EXISTS Y_view;CREATE VIEW X_view AS
SELECT cust, AVG(quant) AS avg_quant
FROM sales
WHERE state = 'NY'
GROUP BY cust;

CREATE VIEW Y_view AS
SELECT cust, AVG(quant) AS avg_quant
FROM sales
WHERE state = 'CT'
GROUP BY cust;

CREATE VIEW Z_view AS
SELECT cust, AVG(quant) AS avg_quant
FROM sales
WHERE state = 'NJ'
GROUP BY cust;

SELECT
    X.cust,
    X.avg_quant AS avg_NY,
    Y.avg_quant AS avg_CT,
    Z.avg_quant AS avg_NJ
FROM X_view X
         JOIN Y_view Y ON X.cust = Y.cust
         JOIN Z_view Z ON X.cust = Z.cust
WHERE X.avg_quant > Y.avg_quant
  AND X.avg_quant > Z.avg_quant;