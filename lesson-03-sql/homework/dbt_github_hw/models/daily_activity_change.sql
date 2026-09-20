-- =====================================================================
-- TASK 4 — daily_activity_change (12 балів). Специфікація: ../../MODELS.md → «daily_activity_change».
-- Зміна кількості подій день-до-дня: LAG(...) OVER (ORDER BY ...).
-- Для першого дня prev_day_events / delta_events = NULL — це очікувано.
-- =====================================================================
WITH daily AS (
    SELECT
        event_date,
        count(*) AS events
    FROM {{ ref('stg_events') }}
    GROUP BY event_date
),
with_prev AS (
    SELECT
        event_date,
        events,
        lag(events) OVER (ORDER BY event_date) AS prev_day_events
    FROM daily
)
SELECT
    event_date,
    events,
    prev_day_events,
    events - prev_day_events AS delta_events
FROM with_prev
ORDER BY event_date
