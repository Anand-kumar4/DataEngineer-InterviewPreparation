-- ✅ Problem 7: Customers Who Bought the Same Product More Than Once

-- 📄 Problem Statement:

-- You are given a purchase history. Identify all customers who have purchased the same product more than once (i.e., duplicate purchases of the same item).

USE PRACTICE_DB;

SELECT * FROM repeat_purchases;

SELECT
	customer_id,
    product
FROM
	repeat_purchases
GROUP BY
	customer_id,
    product
HAVING
	COUNT(*)  > 1;