{{
    config(
        materialized='incremental',
        unique_key='store_id',
        incremental_strategy='merge'
    )
}}

select
    store_id,
    store_name,
    city,
    state,
    region,
    store_type,
    opened_date,
    updated_at,
    current_timestamp() as processed_at
from {{ source('bronze', 'stores') }}
where store_id is not null
{% if is_incremental() %}
  and updated_at > (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
{% endif %}
