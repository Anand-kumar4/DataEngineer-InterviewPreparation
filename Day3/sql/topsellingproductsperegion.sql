-- 🧩 Problem 5: Top-Selling Product Per Region

-- Objective:
-- For each region, identify the product with the highest total sales.

USE practice_db;

DROP TABLE sales;

CREATE TABLE sales (
    sale_id INT,
    region VARCHAR(20),
    product VARCHAR(20),
    amount INT
);

INSERT INTO sales VALUES
(1, 'East', 'iPhone', 500),
(2, 'East', 'iPhone', 600),
(3, 'East', 'iPad', 300),
(4, 'West', 'MacBook', 1000),
(5, 'West', 'iPad', 400),
(6, 'North', 'iPhone', 450),
(7, 'North', 'iPhone', 550),
(8, 'North', 'MacBook', 300);

WITH product_totals AS (
  SELECT
    region,
    product,
    SUM(amount) AS total_sales
  FROM
    sales
  GROUP BY
    region, product
),
ranked_products AS (
  SELECT
    region,
    product,
    total_sales,
    DENSE_RANK() OVER(PARTITION BY region ORDER BY total_sales DESC) AS rnk
  FROM
    product_totals
)
SELECT
  region,
  product,
  total_sales
FROM
  ranked_products
WHERE
  rnk = 1;
	

