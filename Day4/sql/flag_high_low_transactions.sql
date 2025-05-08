-- ✅ Day 4 – Problem 4: Flag High vs Low Value Transactions

-- 📄 Problem Statement:

-- You’re given a transaction dataset with txn_id, user_id, and amount.
-- You need to classify each transaction as:
-- 	•	"High" if the amount ≥ 300
-- 	•	"Low" if the amount < 300

USE PRACTICE_DB;

SELECT * FROM transactions_flag;

SELECT
	txn_id,
    user_id,
    amount,
    CASE 
		WHEN amount < 300 THEN 'Low'
		ELSE 'High'
    END AS value_category
FROM
	transactions_flag;