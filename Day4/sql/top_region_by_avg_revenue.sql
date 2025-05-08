-- ✅ Problem 3: Top Region by Average Revenue per Product

-- 📄 Problem Statement:

-- You’re given a dataset of product sales with three columns: region, product, and amount.
-- Your task is to determine the region that has the highest average revenue for each product.
USE PRACTICE_DB;
SELECT * FROM sales_region;

WITH avg_revenue_calc AS(
	SELECT
		product,
        region,
        AVG(amount) as avg_revenue
	FROM
		sales_region
	GROUP BY
		product,
        region
),
ranked AS (
	SELECT
		product,
        region,
        avg_revenue,
        DENSE_RANK() OVER(PARTITION BY product ORDER BY avg_revenue DESC) as dn
	FROM
		avg_revenue_calc)
SELECT
	product,
    region AS top_region,
    avg_revenue
FROM
	ranked
WHERE
	dn = 1;