-- ✅ Problem 5: Customers with No Orders

-- 📄 Input Files:
-- 	•	customers.csv → customer_id, name
-- 	•	orders.csv → order_id, customer_id

-- ⸻

-- 🧩 Problem Statement:

-- Find all customers who never placed an order.



SELECT * FROM customers;
SELECT * FROM orders;

SELECT
	c.customer_id,
    c.name
FROM
	customers c
LEFT JOIN
	orders o
ON 
	c.customer_id = o.customer_id
WHERE
	o.customer_id IS  NULL;
    
SELECT
	c.customer_id,
    c.name
FROM
	customers c
WHERE
	c.customer_id
NOT IN
	(
    SELECT
		o.customer_id
	FROM
		orders o);