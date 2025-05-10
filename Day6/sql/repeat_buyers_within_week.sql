-- ✅ Problem 5: Detect Repeat Buyers Within a Week

-- 🎯 Goal:

-- Identify customers who made two or more purchases within a 7-day window (i.e., repeat buyers in a short span).

SELECT * FROM repeat_buyers;

WITH with_lag AS(
	SELECT
		customer_id,
        txn_date,
        amount,
        LAG(txn_date, 1) OVER(PARTITION BY customer_id ORDER BY txn_date) as previous_txn_date
	FROM
		repeat_buyers),
with_date_diff AS(
	SELECT
		customer_id,
        txn_date,
        amount,
        previous_txn_date,
        DATEDIFF(txn_date, previous_txn_date) AS gap_between_txns
	FROM
		with_lag)
SELECT
	DISTINCT customer_id
FROM
	with_date_diff
WHERE
	gap_between_txns <= 7;