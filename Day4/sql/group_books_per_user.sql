-- # ✅ Problem 1: Group Books per User

-- # 📄 Problem Statement:

-- # You are given a dataset of users and the books they’ve purchased. 
-- # Your goal is to return one row per user, where the books are aggregated into a comma-separated list. If a user hasn’t purchased any book (i.e., value is NULL), show NULL.

SELECT * FROM Books_purchased;

SELECT
	Name,
    GROUP_CONCAT(Books_purchased ORDER BY Books_purchased SEPARATOR ',') AS Books_purchased
FROM
	Books_purchased
GROUP BY
	Name;