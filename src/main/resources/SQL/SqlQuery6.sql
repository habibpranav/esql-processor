DROP VIEW IF EXISTS X_view;
DROP VIEW IF EXISTS Y_view;

CREATE VIEW X_view AS
SELECT prod, COUNT(*) as count_x
FROM sales
GROUP BY prod;

-- Y: for each (prod, quant), count how many sales have lower quant
CREATE VIEW Y_view AS
SELECT s1.prod, s1.quant, COUNT(s2.prod) as count_y
FROM (SELECT DISTINCT prod, quant FROM sales) s1
         LEFT JOIN sales s2 ON s1.prod = s2.prod AND s2.quant < s1.quant
GROUP BY s1.prod, s1.quant;

-- Join and apply HAVING condition
SELECT Y.prod, Y.quant
FROM Y_view Y
         JOIN X_view X ON Y.prod = X.prod
WHERE Y.count_y = X.count_x / 2;