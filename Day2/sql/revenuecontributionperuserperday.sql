-- ✅ Problem 5: Revenue Contribution % Per Product Per Day

-- 📄 Input File: daily_sales.csv
-- 📌 Columns: sale_date, product_id, revenue

-- ⸻

-- 🧩 Problem Statement:

-- For each day, calculate the percentage contribution of each product’s revenue to that day’s total revenue.

SELECT * FROM daily_sales;

SELECT
	sale_date,
    product_id,
    revenue,
    SUM(revenue) OVER(PARTITION BY sale_date) as daily_total,
    ROUND((revenue / SUM(revenue) OVER(PARTITION BY sale_date))*100, 2) as revenue_pct
FROM
	daily_sales;