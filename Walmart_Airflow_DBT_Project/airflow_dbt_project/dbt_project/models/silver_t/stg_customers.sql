{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}

select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    signup_date,
    loyalty_tier,
    updated_at,
    current_timestamp() as processed_at
from {{ source('bronze', 'customers') }}
where customer_id is not null
{% if is_incremental() %}
  and updated_at > (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
{% endif %}
