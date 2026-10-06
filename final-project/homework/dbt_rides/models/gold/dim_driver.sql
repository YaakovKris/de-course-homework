-- gold.dim_driver — ЕТАП 2. Grain: водій. SPEC.md, розділ 4.4.
--   * incremental, unique_key='driver_key', delete+insert; за подіями ride_accepted у ref('events')
--   * «останній» рейтинг — за occurred_at; перераховуйте водія з УСІЄЇ його історії, не з батча
--   * плюс член driver_key = 'unknown' (поїздки, скасовані до прийняття)

{{ config(materialized='incremental', unique_key='driver_key', incremental_strategy='delete+insert') }}

with accepted as (
    select
        payload #>> '{driver,id}'                  as driver_key,
        payload #>> '{driver,vehicle,type}'         as vehicle_type,
        payload #>> '{driver,vehicle,medallion}'    as medallion,
        (payload #>> '{driver,rating}')::numeric    as rating,
        occurred_at,
        _ingested_at
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
),

{% if is_incremental() %}
-- Запізніла подія зі старим occurred_at не повинна затерти новіше значення: перераховуємо
-- водія з УСІЄЇ його історії, а не лише з нового батча.
affected_drivers as (
    select distinct driver_key from accepted where _ingested_at > {{ high_watermark() }}
),
relevant as (
    select a.* from accepted as a inner join affected_drivers as d using (driver_key)
)
{% else %}
relevant as (
    select * from accepted
)
{% endif %}

, latest as (
    -- «Останній» = за occurred_at, а не за порядком прибуття.
    select distinct on (driver_key)
        driver_key,
        vehicle_type,
        medallion,
        rating as latest_rating
    from relevant
    order by driver_key, occurred_at desc
),

span as (
    select
        driver_key,
        min(occurred_at)  as first_seen_at,
        max(occurred_at)  as last_seen_at,
        max(_ingested_at) as _ingested_at
    from relevant
    group by driver_key
)

select
    l.driver_key,
    l.vehicle_type,
    l.medallion,
    l.latest_rating,
    s.first_seen_at,
    s.last_seen_at,
    s._ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from latest as l
inner join span as s using (driver_key)

union all

-- Член для поїздок, скасованих до прийняття (driver_id ще не визначено). Рівно один рядок
-- після будь-якої кількості запусків: delete+insert за unique_key замінює його щоразу.
select
    'unknown'         as driver_key,
    'unknown'         as vehicle_type,
    null::text        as medallion,
    null::numeric     as latest_rating,
    null::timestamptz as first_seen_at,
    null::timestamptz as last_seen_at,
    null::timestamptz as _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
