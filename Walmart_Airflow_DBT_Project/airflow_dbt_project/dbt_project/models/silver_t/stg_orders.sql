{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

select
    order_id,
    customer_id,
    store_id,
    employee_id,
    order_date,
    order_status,
    updated_at,
    current_timestamp() as processed_at
from {{ source('bronze', 'orders') }}
where order_id is not null
{% if is_incremental() %}
  and updated_at > (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
{% endif %}
