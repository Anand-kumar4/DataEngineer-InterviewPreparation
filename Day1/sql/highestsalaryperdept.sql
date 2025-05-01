-- ✅ Problem 2: Highest Salary per Department

-- 📄 Input File: employees.csv
-- 📌 Columns: emp_id, name, department, salary

-- ⸻

-- 🧩 Problem Statement:

-- From the employees table, find the employee(s) with the highest salary in each department.
-- If multiple employees share the top salary in the same department, include them all.

SELECT * FROM employees;

SELECT
	emp_id,
    name,
    department,
    salary
FROM
	(
SELECT
	emp_id,
    name,
    department,
    salary,
    dense_rank() over(partition by department order by salary desc) as rnk
FROM
	employees) as rn
WHERE
	rnk = 1;
    