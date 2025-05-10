-- # 💡 Problem:

-- # Find the customer(s) with the highest total monthly spend.

SELECT * FROM monthly_transactions;

WITH monthly_year AS (
  SELECT
    customer_id,
    txn_date,
    DATE_FORMAT(txn_date, '%Y-%m') AS month_year,
    amount
  FROM monthly_transactions
),
monthly_year_spend AS (
  SELECT
    customer_id,
    month_year,
    SUM(amount) AS total_monthly_spend
  FROM monthly_year
  GROUP BY customer_id, month_year
),
ranked AS (
  SELECT
    customer_id,
    month_year,
    total_monthly_spend,
    DENSE_RANK() OVER (PARTITION BY month_year ORDER BY total_monthly_spend DESC) AS rnk
  FROM monthly_year_spend
)
SELECT
  customer_id,
  month_year,
  total_monthly_spend
FROM ranked
WHERE rnk = 1;
		