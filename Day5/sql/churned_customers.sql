-- # 🔄 Problem 7 : Churn Detection – Customers with No Purchase in Last 30 Days

-- # Dataset: churn_check.csv

-- # 🎯 Goal:

-- # Find customers who did not make any purchase in the last 30 days from a reference date (e.g., 2024-04-10).


SELECT customer_id
FROM (
  SELECT 
    customer_id,
    MAX(purchase_date) AS last_purchase
  FROM churn_check
  GROUP BY customer_id
) AS latest
WHERE last_purchase < DATE_SUB('2024-04-10', INTERVAL 30 DAY);