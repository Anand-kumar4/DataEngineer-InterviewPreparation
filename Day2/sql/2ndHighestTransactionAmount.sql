-- ✅ Problem 1: Second Highest Transaction Amount per User

-- 📄 Input File: transactions.csv
-- 📌 Columns: user_id, amount, txn_date

-- ⸻

-- 🧩 Problem Statement:

-- For each user, find the second highest transaction amount.


SELECT
	user_id,
    amount,
    txn_date
FROM (
SELECT
	user_id,
    amount,
    txn_date,
    DENSE_RANK() OVER(PARTITION BY user_id ORDER BY amount DESC) as rnk
FROM
	transactions) dn
WHERE
	rnk = 2;
