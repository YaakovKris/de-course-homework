-- gold.agg_zone_hourly — ЕТАП 2. Grain: (requested_hour, pickup_zone_key). SPEC.md, розділ 4.4.
--   * incremental; після будь-якого інкременту = перерахунок із fact_ride рядок у рядок
--   * подумайте: що перераховувати, коли поїздка змінила зону? Який ключ стабільний?

-- requested_hour стабільний (походить від requested_at, який не змінюється); pickup_zone_key —
-- ні (ride_started може принести фактичну зону пізніше). Тому при інкременті перераховуємо
-- ВСІ зони в межах кожної зачепленої години — так стара зона (звідки поїздка «пішла») і нова
-- (куди «прийшла») обидві отримують правильні числа з одного й того самого перерахунку.
-- pre_hook прибирає ВСІ рядки зачеплених годин (не лише ті зони, що лишились непорожніми):
-- звичайний delete+insert видаляє рядок лише якщо його ключ є в НОВИХ даних, тож зона, яка
-- втратила останню поїздку (побіжала в іншу), у новому результаті відсутня — і без цього
-- pre_hook її застарілий рядок лишився б у вітрині назавжди.
{{ config(
    materialized='incremental',
    unique_key=['requested_hour', 'pickup_zone_key'],
    incremental_strategy='delete+insert',
    pre_hook="{% if is_incremental() %}delete from {{ this }} where requested_hour in (select distinct requested_hour from {{ ref('fact_ride') }} where _ingested_at > (select coalesce(max(_ingested_at), '-infinity'::timestamptz) from {{ this }})){% endif %}"
) }}

with fact as (
    select * from {{ ref('fact_ride') }}
),

{% if is_incremental() %}
affected_hours as (
    select distinct requested_hour from fact where _ingested_at > {{ high_watermark() }}
),
relevant as (
    select f.* from fact as f inner join affected_hours as h using (requested_hour)
)
{% else %}
relevant as (
    select * from fact
)
{% endif %}

select
    requested_hour,
    pickup_zone_key,
    count(*)                                                       as rides_requested,
    count(*) filter (where status = 'completed')                   as rides_completed,
    count(*) filter (where status = 'cancelled')                   as rides_cancelled,
    coalesce(sum(total_amount) filter (where status = 'completed'), 0)::numeric(12, 2) as gross_revenue,
    coalesce(sum(tip_amount)   filter (where status = 'completed'), 0)::numeric(12, 2) as tips,
    avg(wait_seconds)                                              as avg_wait_seconds,
    max(_ingested_at)                                              as _ingested_at,
    '{{ run_started_at }}'::timestamptz                            as _loaded_at
from relevant
group by requested_hour, pickup_zone_key
