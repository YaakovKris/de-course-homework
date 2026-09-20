{{ config(materialized='incremental', incremental_strategy='append') }}

with source as (
    select
        id as event_id,
        type as event_type,
        actor.login as actor_login,
        repo.name as repo_name,
        split(repo.name, '/')[0] as repo_owner,
        to_timestamp(created_at) as created_at,
        payload,
        _ingested_at,
        _source_file
    from {{ source('bronze', 'raw_events') }}
    where type in (
        'PushEvent',
        'PullRequestEvent',
        'IssuesEvent',
        'IssueCommentEvent',
        'WatchEvent',
        'ForkEvent'
    )
      and public is true
      and id is not null
      and repo.name is not null
      and created_at is not null
      {% if is_incremental() %}
      -- фільтр застосовується ДО window-функції, а не після — інкрементальний
      -- запуск ранжує лише нові рядки bronze, а не всю таблицю щоразу.
      -- Безпечно: кожен event_id потрапляє в bronze рівно в одному інжест-батчі,
      -- тож дедуп у межах нового зрізу не втрачає дублікатів зі старих запусків.
      and _ingested_at > (
          select coalesce(max(_ingested_at), cast('1970-01-01 00:00:00' as timestamp))
          from {{ this }}
      )
      {% endif %}
),
ranked as (
    select
        *,
        row_number() over (partition by event_id order by _ingested_at desc, _source_file desc) as rn
    from source
)
select
    event_id,
    event_type,
    actor_login,
    repo_name,
    repo_owner,
    created_at,
    payload,
    _ingested_at,
    _source_file
from ranked
where rn = 1
