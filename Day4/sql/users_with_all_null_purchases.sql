-- ✅ Problem 2: Find Users with All NULL Purchases

-- 📄 Problem Statement:
-- You’re given a dataset of users and the books they attempted to purchase.
-- Your task is to find users for whom every entry in Books_purchased is NULL.



SELECT * FROM books_purchased_problem2;

SELECT
  name
FROM
  books_purchased_problem2
GROUP BY
  name
HAVING
  COUNT(Books_purchased) = 0;

