-- ✅ Problem 3: Detect Customers with Consecutive Purchase Days

-- 📄 Dataset: consecutive_purchases.csv

-- 🎯 Goal:

-- Find customers who made purchases on two or more consecutive calendar days (e.g., 2024-04-01 and 2024-04-02).

SELECT * FROM consecutive_purchases;

WITH ranked AS(
SELECT
	customer_id,
    purchase_date,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY purchase_date) as rn
FROM
	consecutive_purchases
),
grouped AS(
SELECT
	customer_id,
    purchase_date,
    DATEDIFF(purchase_date, '1970-01-01') - rn as date_diff
FROM
	ranked
    )
SELECT
	DISTINCT customer_id
FROM
    (
SELECT
	customer_id,
    date_diff
FROM
	grouped
GROUP BY
	customer_id,
    date_diff
HAVING
	COUNT(*) >= 2) inner_query;