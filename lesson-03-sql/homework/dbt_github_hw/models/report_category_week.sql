-- =====================================================================
-- TASK 7 — report_category_week (20 балів). Специфікація: ../../MODELS.md → «report_category_week».
--
-- Оптимізована версія report_category_week_naive: результат ІДЕНТИЧНИЙ, план — ні.
-- Єдина змістовна зміна — join по СИРІЙ партиційній колонці замість strftime-рядка:
--     naive:  ON strftime(e.event_date, '%Y-%m-%d') = strftime(c.day, '%Y-%m-%d')
--     тут:    ON e.event_date = c.day
-- Ключ join більше не загорнутий у функцію, тому DuckDB може:
--   1) пропагувати фільтр c.iso_week = 2 через join на e.event_date (filter propagation);
--   2) виконати partition pruning → читаються лише 7 партицій (8–14 січня) замість 14.
-- =====================================================================
SELECT
    c.iso_week,
    cat.category,
    count(*) AS events
FROM {{ ref('stg_events') }} e
JOIN {{ ref('calendar') }} c
    ON e.event_date = c.day
JOIN {{ ref('event_categories') }} cat
    ON e.event_type = cat.event_type
WHERE c.iso_week = 2
GROUP BY c.iso_week, cat.category
ORDER BY cat.category
