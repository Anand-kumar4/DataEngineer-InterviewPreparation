-- ✅ Problem 4: Running Total of Sales

-- 📄 Input File: sales.csv
-- 📌 Columns: date, product_id, revenue

-- ⸻

-- 🧩 Problem Statement:

-- For each product, compute the running total of revenue over time ordered by date.




SELECT
	date,
    product_id,
    revenue,
    SUM(revenue) OVER(PARTITION BY product_id ORDER BY date)
FROM
	sales;