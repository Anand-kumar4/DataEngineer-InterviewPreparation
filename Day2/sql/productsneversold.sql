-- ✅ Problem 4: Products That Have Never Been Sold

-- 📄 Input Files:
-- 	•	products.csv → product_id, product_name
-- 	•	sales.csv → sale_id, product_id, quantity, sale_date

-- ⸻

-- 🧩 Problem Statement:

-- From the list of products, find the ones that have never been sold (i.e., their product_id doesn’t appear in sales).


-- Approach 1
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

-- Approach 2
SELECT
	p.product_id,
    p.product_name
FROM
	products p
WHERE
	p.product_id NOT IN(
SELECT
	s.product_id
FROM
	sales s
);