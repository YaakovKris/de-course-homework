-- Перевірка task 1: жоден «брудний» рядок із сирих даних не потрапив у stg_events.
-- На відміну від попередньої версії, критерій «брудний» рахується тут напряму з
-- сирого Parquet (read_parquet), а не з уже відфільтрованих колонок stg_events —
-- інакше тест лише повторював WHERE моделі й не міг впіймати реальний баг у фільтрі.
-- Має повертати 0 рядків.
WITH dirty_raw AS (
    SELECT id
    FROM read_parquet('{{ var("events_path") }}', hive_partitioning = true)
    WHERE event_type NOT IN ('PushEvent', 'IssuesEvent', 'PullRequestEvent', 'WatchEvent', 'IssueCommentEvent')
       OR actor_login LIKE '%[bot]'
       OR (event_type = 'PushEvent' AND payload_commit_count = 0)
)
SELECT stg.id
FROM {{ ref('stg_events') }} stg
JOIN dirty_raw ON dirty_raw.id = stg.id
