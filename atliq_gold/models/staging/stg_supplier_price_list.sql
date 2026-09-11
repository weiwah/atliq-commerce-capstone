SELECT product_id, supplier_cost
FROM {{ source('silver', 'supplier_price_list') }}