-- # ✅ Problem 1: First Purchase per Customer

-- # 📄 Input File: transactions.csv
-- # 📌 Columns: customer_id, txn_date, amount

-- # ⸻

-- # 🧩 Problem Statement:

-- # From the transactions data, find the first purchase (i.e. earliest txn_date) for each customer along with the amount.


CREATE DATABASE MAY1;
USE MAY1;

SELECT * FROM transactions;

SELECT
	customer_id,
    txn_date,
    amount
FROM (
SELECT
	customer_id,
    txn_date,
    amount,
    row_number() over(partition by customer_id order by txn_date asc) as row_num
FROM
	transactions) as rn
WHERE
	row_num = 1;