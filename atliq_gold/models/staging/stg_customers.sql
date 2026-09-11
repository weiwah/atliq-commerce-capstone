SELECT customer_id, customer_name, city, signup_date
FROM {{ source('silver', 'customers') }}