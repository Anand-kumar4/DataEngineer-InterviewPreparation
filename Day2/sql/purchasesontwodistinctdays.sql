-- ✅ Problem 2: Users with Purchases on Exactly 2 Distinct Days

-- 📄 Input File: user_txns.csv
-- 📌 Columns: user_id, txn_date

-- ⸻

-- 🧩 Problem Statement:

-- Find users who made purchases on exactly 2 different days.


SELECT 
	DISTINCT user_id
FROM
	(
SELECT 
	user_id,
    COUNT(DISTINCT txn_date) as count_distinct_transactions
FROM
	user_txns
GROUP BY
	user_id) cnt
WHERE
	count_distinct_transactions = 2;