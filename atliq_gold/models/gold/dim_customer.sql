SELECT
    customer_id,
    customer_name,
    city,
    signup_date,
    DATE_TRUNC('MONTH', signup_date) AS signup_cohort
FROM {{ ref('stg_customers') }}