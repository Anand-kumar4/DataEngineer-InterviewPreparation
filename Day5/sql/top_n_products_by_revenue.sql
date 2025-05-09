-- ✅ Problem 4: Top 2 Products by Total Revenue

-- 📄 Dataset: top_products.csv

-- 🎯 Goal:

-- Find the top 2 products with the highest total revenue across all transactions.

SELECT * FROM top_products;

WITH total_revenue_calc AS(
SELECT
	product_id,
    SUM(revenue) as total_revenue
FROM
	top_products
GROUP BY
	product_id),
ranked AS(
SELECT
	product_id,
    total_revenue,
    DENSE_RANK() OVER(ORDER BY total_revenue DESC) as rn
FROM
	total_revenue_calc)
SELECT
	product_id,
    total_revenue
FROM
	ranked
WHERE
	rn <= 2;