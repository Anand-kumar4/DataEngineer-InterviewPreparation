-- # ✅ Problem 1: Second Purchase Date per Customer

-- # 📄 Dataset: second_purchase.csv

-- # 🎯 Goal:

-- # For each customer, return the second purchase date (i.e., the second earliest transaction by purchase_date).

USE PRACTICE_DB;

SELECT * FROM second_purchase;

SELECT
	customer_id,
    purchase_date as second_purchase_date
FROM
(SELECT
	customer_id,
    purchase_date,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY purchase_date) as rn
FROM
	second_purchase) row_num
WHERE
	rn = 2;