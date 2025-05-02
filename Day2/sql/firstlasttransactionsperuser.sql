-- ✅ Problem 3: First and Last Login Timestamp per User

-- 📄 Input File: logins.csv
-- 📌 Columns: user_id, login_time

-- ⸻

-- 🧩 Problem Statement:

-- For each user, return their first login timestamp and last login timestamp.

SELECT
	user_id,
    MIN(login_time) AS first_login,
    MAX(login_time) AS last_login
FROM
	logins
GROUP BY
	user_id;
	