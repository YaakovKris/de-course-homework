-- Числові величини завершених поїздок не можуть бути відʼємними: відʼємна сума, дистанція чи
-- тривалість — ознака зламаного парсингу payload (наприклад, переплутаний знак або не той
-- JSON-шлях), а не легітимні дані джерела.
select ride_id, fare_amount, tolls_amount, tip_amount, total_amount, distance_km, wait_seconds, trip_seconds
from {{ ref('fact_ride') }}
where status = 'completed'
  and (
        fare_amount < 0
     or tolls_amount < 0
     or coalesce(tip_amount, 0) < 0
     or total_amount < 0
     or distance_km < 0
     or wait_seconds < 0
     or trip_seconds < 0
  )
