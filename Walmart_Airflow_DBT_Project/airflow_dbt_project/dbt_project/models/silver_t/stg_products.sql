{{
    config(
        materialized='incremental',
        unique_key='product_id',
        incremental_strategy='merge'
    )
}}

select
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    unit_price,
    is_active,
    updated_at,
    current_timestamp() as processed_at
from {{ source('bronze', 'products') }}
where product_id is not null
  and unit_price > 0
{% if is_incremental() %}
  and updated_at > (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
{% endif %}
