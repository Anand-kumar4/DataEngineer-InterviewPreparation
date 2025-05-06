CREATE TABLE transactions (
  customer_id INT,
  txn_date DATE,
  amount INT
);

INSERT INTO transactions (customer_id, txn_date, amount) VALUES
(101, '2024-05-01', 100),
(101, '2024-05-02', 200),
(101, '2024-05-03', 150),  -- not strictly increasing
(102, '2024-05-01', 50),
(102, '2024-05-02', 80),
(102, '2024-05-03', 120), -- strictly increasing
(103, '2024-05-01', 90),
(103, '2024-05-02', 90),  -- same value, not increasing
(104, '2024-05-01', 10);   -- only 1 txn


WITH ranked_txns AS (
  SELECT
    customer_id,
    txn_date,
    amount,
    LAG(amount) OVER (PARTITION BY customer_id ORDER BY txn_date) AS previous_amount
  FROM transactions
),
flags AS (
  SELECT
    customer_id,
    CASE
      WHEN previous_amount IS NULL THEN NULL
      WHEN amount > previous_amount THEN 1
      ELSE 0
    END AS is_increasing
  FROM ranked_txns
),
final_check AS (
  SELECT
    customer_id,
    SUM(is_increasing) AS increasing_count,
    COUNT(is_increasing) AS total_comparisons
  FROM flags
  GROUP BY customer_id
)
SELECT customer_id
FROM final_check
WHERE increasing_count = total_comparisons;