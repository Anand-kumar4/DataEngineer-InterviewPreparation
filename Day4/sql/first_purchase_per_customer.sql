-- ✅ Problem Statement:

-- 📝 Task:
-- For each customer, find the first product they purchased (based on earliest purchase_date).

USE PRACTICE_DB;

SELECT * FROM first_purchases;

SELECT
	customer_id,
    product,
    purchase_date
FROM(
SELECT
	customer_id,
    product,
    purchase_date,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY purchase_date) as rn
FROM
	first_purchases) row_num
WHERE
	rn = 1
;