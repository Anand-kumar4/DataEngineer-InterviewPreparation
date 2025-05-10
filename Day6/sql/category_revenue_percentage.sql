-- ✅ Problem 4: Category-wise Revenue Contribution %

-- 🎯 Goal:

-- For each category, compute the revenue contribution percentage of each product within that category.

SELECT * FROM category_sales;

WITH category_agg AS(
SELECT
	category,
    SUM(revenue) AS category_total
FROM
	category_sales
GROUP BY
	category),
joined AS(
SELECT
	c.category,
    c.product_id,
    c.revenue,
    cg.category_total
FROM
	category_sales c
INNER JOIN
	category_agg cg
ON
	c.category = cg.category
    )
SELECT
	category,
    product_id,
    revenue,
    category_total,
    round((revenue) * 100 / (category_total)) as contribution_pct
FROM
	joined
ORDER BY category, contribution_pct DESC;