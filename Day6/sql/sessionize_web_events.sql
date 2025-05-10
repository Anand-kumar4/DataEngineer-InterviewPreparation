-- ✅ Problem 6: Sessionize Web Events

-- 🎯 Goal:
-- Group user events into sessions, where a session is defined as a gap of more than 30 minutes between events.

WITH with_lag AS (
  SELECT
    user_id,
    event_time,
    page,
    LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS previous_event_time
  FROM
    web_events
),
with_gap AS (
  SELECT
    *,
    TIMESTAMPDIFF(MINUTE, previous_event_time, event_time) AS gap_minutes
  FROM
    with_lag
),
session_flags AS (
  SELECT
    *,
    CASE
      WHEN gap_minutes IS NULL OR gap_minutes > 30 THEN 1
      ELSE 0
    END AS session_start
  FROM
    with_gap
),
final_sessions AS (
  SELECT
    *,
    SUM(session_start) OVER (
      PARTITION BY user_id
      ORDER BY event_time
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_id
  FROM
    session_flags
)
SELECT
  user_id,
  event_time,
  page,
  session_id
FROM
  final_sessions
ORDER BY
  user_id,
  event_time;