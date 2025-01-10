{{ config(materialized='table', schema='presentation') }}

with final as (
    SELECT
        pu.store_id,
        pr.category_id,
        SUM(pi.product_price * pi.product_count) AS sales_sum
    FROM
        {{ source('postgres_business', 'purchase_items') }} pi
    JOIN
        {{ source('postgres_business', 'products') }} pr ON pi.product_id = pr.product_id
    JOIN
        {{ source('postgres_business', 'purchases') }} pu ON pi.purchase_id = pu.purchase_id
    GROUP BY
        pu.store_id,
        pr.category_id
)
select *
from final