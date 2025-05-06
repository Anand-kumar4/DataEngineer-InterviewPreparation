-- ✅ Task:

-- Write a PySpark program to return top 2 most recent orders for each customer based on order_date.



SELECT * FROM orders;

SELECT
	order_id,
    customer_id
FROM
	(
SELECT
	order_id,
    customer_id,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY order_date desc) as row_num
FROM
	orders) as ranked
WHERE
	row_num <= 2;