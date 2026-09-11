WITH dates AS (
    SELECT explode(
        sequence(
            CAST('2024-01-01' AS DATE),
            CAST('2026-12-31' AS DATE),
            INTERVAL 1 DAY
        )
    ) AS date_day
)

SELECT
    date_day,
    day(date_day) AS day,
    month(date_day) AS month,
    quarter(date_day) AS quarter,
    year(date_day) AS year,
    date_format(date_day, 'EEEE') AS weekday
FROM dates