{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        incremental_strategy='merge'
    )
}}

-- Granular, additive measures at the order-line grain, with FKs into the
-- SCD Type 2 dimension tables built by dbt snapshot.
select
    o.order_item_id,
    o.order_id,
    o.order_date,
    o.order_status,
    o.customer_id,
    o.store_id,
    o.employee_id,
    o.product_id,
    o.quantity,
    o.unit_price,
    o.line_amount,
    (o.line_amount) as total_amount,
    current_timestamp() as processed_at
from {{ ref('obt') }} as o
{% if is_incremental() %}
where o.order_date >= (select coalesce(max(order_date), '1900-01-01') from {{ this }})
{% endif %}
