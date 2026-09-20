-- =====================================================================
-- TASK 5 — starred_repos_without_push (12 балів). Специфікація: ../../MODELS.md → «starred_repos_without_push».
-- Репозиторії зі зіркою (WatchEvent), але без жодного PushEvent — anti-join через NOT EXISTS
-- (NOT IN тут небезпечний: NULL у підзапиті «з'їв» би весь результат).
-- =====================================================================
WITH starred AS (
    SELECT DISTINCT repo_name
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'WatchEvent'
)
SELECT s.repo_name
FROM starred s
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('stg_events') }} p
    WHERE p.event_type = 'PushEvent'
      AND p.repo_name = s.repo_name
)
ORDER BY s.repo_name
