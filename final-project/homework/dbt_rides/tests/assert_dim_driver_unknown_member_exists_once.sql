-- Член 'unknown' у gold.dim_driver (поїздки, скасовані до прийняття) має існувати рівно один
-- раз, незалежно від кількості запусків. HAVING над агрегатом без GROUP BY ловить і 0, і 2+:
-- порожній результат select (член зник, наприклад якщо UNION ALL випадково потрапив під
-- affected_drivers-фільтр) не пройшов би повз group-by-варіант тесту, а цей — проходить.
select count(*) as n
from {{ ref('dim_driver') }}
where driver_key = 'unknown'
having count(*) <> 1
