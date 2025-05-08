-- ✅ Problem 5: Customers with Exactly 2 Distinct Purchase Amounts

-- 📄 Problem Statement:

-- You’re given a dataset of customer transactions. Your task is to identify customers who have made exactly 2 distinct transaction amounts.

USE PRACTICE_DB;

SELECT * FROM distinct_amounts;

SELECT  
    customer_id,
    COUNT(DISTINCT amount) as cnt
FROM
	distinct_amounts
GROUP BY
	customer_id
HAVING
	cnt = 2;
    
SELECT customer_id
FROM distinct_amounts
GROUP BY customer_id
HAVING COUNT(DISTINCT amount) = 2;
