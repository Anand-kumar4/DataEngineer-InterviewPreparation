USE practice_db;

-- Create tables
CREATE TABLE products (
    product_id INT,
    product_name VARCHAR(50)
);

CREATE TABLE sales (
    sale_id INT,
    product_id INT,
    revenue INT,
    sale_date DATE
);

-- Insert into products
INSERT INTO products (product_id, product_name) VALUES
(101, 'iPhone 15'),
(102, 'MacBook Pro'),
(103, 'AirPods Pro'),
(104, 'iPad Air');

-- Insert into sales
INSERT INTO sales (sale_id, product_id, revenue, sale_date) VALUES
(1, 101, 300, '2024-04-10'),
(2, 103, 100, '2024-04-10'),
(3, 101, 400, '2024-04-11');


SELECT
	p.product_id,
    p.product_name
FROM
	products p
LEFT JOIN
	sales s
ON
	p.product_id = s.product_id
WHERE
	s.product_id IS NULL;