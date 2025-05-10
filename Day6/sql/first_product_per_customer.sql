-- 🎯 Goal:

-- For each customer, identify the first product they purchased, and join this info back to the original orders table.

SELECT * FROM orders_with_products;

-- 🧮 Step 1: Identify the first product purchased per customer using ROW_NUMBER()
WITH first_orders AS(
	SELECT
		customer_id,
        product,
        ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY order_date ASC) as row_num
	FROM
		orders_with_products
	)
-- 🔗 Step 2: Join original orders_with_products with the first product info per customer
SELECT
	o.*,
    f.product as first_product
FROM
	orders_with_products o
LEFT JOIN
	first_orders f
ON
	o.customer_id = f.customer_id
WHERE
	f.row_num = 1;


-- 🧮 Optimized version: Filter for first product inside subquery to reduce join size
WITH first_orders AS (
  SELECT
    customer_id,
    product
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date ASC) AS row_num
    FROM orders_with_products
  ) sub
  WHERE row_num = 1
)
-- 🔗 Join original table with filtered first product set
SELECT
  o.*,
  f.product AS first_product
FROM
  orders_with_products o
LEFT JOIN
  first_orders f ON o.customer_id = f.customer_id;