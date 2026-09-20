{{ config(materialized='view') }}
-- =====================================================================
-- TASK 1 — stg_events (12 балів). Специфікація: ../../MODELS.md → «stg_events».
-- Чистий шар поверх сирих подій: schema-on-read з партиційованого Parquet + DQ-фільтри.
-- Матеріалізація — view, без window-функцій, щоб partition pruning працював наскрізь
-- (це критично для Task 7).
-- =====================================================================
SELECT
    id,
    event_type,
    created_at,
    event_date,
    actor_login,
    repo_name,
    payload_commit_count,
    payload_action,
    payload_ref
FROM read_parquet('{{ var("events_path") }}', hive_partitioning = true)
WHERE event_type IN (
        'PushEvent', 'IssuesEvent', 'PullRequestEvent', 'WatchEvent', 'IssueCommentEvent'
    )
  -- прибрати ботів: actor_login на кшталт 'dependabot[bot]'
  AND actor_login NOT LIKE '%[bot]'
  -- прибрати «порожні» push-и без комітів
  AND NOT (event_type = 'PushEvent' AND payload_commit_count = 0)
