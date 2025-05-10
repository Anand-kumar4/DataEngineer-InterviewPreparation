-- ✅ Problem 7: Product Category Popularity Trends

-- 🎯 Goal:
-- Track monthly sales per product category, and identify trend direction compared to the previous month.

WITH monthly_agg AS (
  SELECT
    category,
    DATE_FORMAT(sale_date, '%Y-%m') AS month,
    SUM(revenue) AS total_revenue
  FROM
    category_monthly_sales
  GROUP BY
    category, DATE_FORMAT(sale_date, '%Y-%m')
),
ranked_with_lag AS (
  SELECT
    *,
    LAG(total_revenue) OVER (PARTITION BY category ORDER BY month) AS previous_month_revenue
  FROM
    monthly_agg
)
SELECT
  category,
  month,
  total_revenue,
  CASE
    WHEN previous_month_revenue IS NULL THEN '-'
    WHEN total_revenue > previous_month_revenue THEN 'UP'
    WHEN total_revenue < previous_month_revenue THEN 'DOWN'
    ELSE 'SAME'
  END AS trend
FROM
  ranked_with_lag
ORDER BY
  category, month;
