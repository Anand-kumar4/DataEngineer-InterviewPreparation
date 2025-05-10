-- ✅ Problem 2: Order-to-Delivery Duration

-- 🎯 Goal:

-- For each order, calculate the number of days it took to deliver the order.

SELECT * FROM orders_delivery;

SELECT
	order_id,
    customer_id,
    DATEDIFF(delivery_date, order_date) AS duration_days
FROM
	orders_delivery;