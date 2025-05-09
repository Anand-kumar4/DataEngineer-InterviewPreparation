-- ✅ Problem 2: Running Average of Sales per Product

-- 📄 Dataset: sales_avg.csv

-- 🎯 Goal:

-- For each product, compute the running average of revenue ordered by sale_date.

SELECT * FROM sales_avg;

SELECT
	product_id,
    sale_date,
    revenue,
    AVG(revenue) OVER(PARTITION BY product_id ORDER BY sale_date) as running_avg
FROM
	sales_avg;