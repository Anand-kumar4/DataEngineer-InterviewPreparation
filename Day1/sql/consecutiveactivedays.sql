-- ✅ Problem 3: Consecutive Active Days

-- 📄 Input File: user_activity.csv
-- 📌 Columns: user_id, activity_date

-- ⸻

-- 🧩 Problem Statement:

-- From the user_activity data, find users who were active for 3 or more consecutive days.


SELECT * FROM user_activity;

WITH numbered AS(
	SELECT
		user_id,
        activity_date,
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY activity_date) as rn,
        DATEDIFF(activity_date, '1970-01-01') - ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY activity_date) AS grp
	FROM
		user_activity)
SELECT DISTINCT user_id
FROM(
SELECT
	user_id,
    count(*) as streak_lenghth
FROM
	numbered
GROUP BY
	user_id,
    grp
HAVING
	count(*) >= 3) t;