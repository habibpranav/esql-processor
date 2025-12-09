DROP VIEW IF EXISTS X_view;
DROP VIEW IF EXISTS Y_view;

CREATE VIEW X_view AS
SELECT cust, prod, AVG(quant) as avg_x
FROM sales
GROUP BY cust, prod;

-- Y: average quant for same product but OTHER customers
CREATE VIEW Y_view AS
SELECT s1.cust, s1.prod, AVG(s2.quant) as avg_y
FROM (SELECT DISTINCT cust, prod FROM sales) s1
         JOIN sales s2 ON s1.prod = s2.prod AND s1.cust <> s2.cust
GROUP BY s1.cust, s1.prod;

-- Join them
SELECT X.cust, X.prod, X.avg_x, Y.avg_y
FROM X_view X
         JOIN Y_view Y ON X.cust = Y.cust AND X.prod = Y.prod;