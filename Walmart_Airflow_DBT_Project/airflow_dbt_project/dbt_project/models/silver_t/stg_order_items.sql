{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        incremental_strategy='merge'
    )
}}

select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    (quantity * unit_price) as line_amount,
    updated_at,
    current_timestamp() as processed_at
from {{ source('bronze', 'order_items') }}
where order_item_id is not null
  and quantity > 0
  and unit_price > 0
{% if is_incremental() %}
  and updated_at > (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
{% endif %}
