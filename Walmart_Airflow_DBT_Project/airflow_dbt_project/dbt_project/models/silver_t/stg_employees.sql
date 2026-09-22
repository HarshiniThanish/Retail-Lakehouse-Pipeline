{{
    config(
        materialized='incremental',
        unique_key='employee_id',
        incremental_strategy='merge'
    )
}}

select
    employee_id,
    first_name,
    last_name,
    store_id,
    role,
    hire_date,
    is_active,
    updated_at,
    current_timestamp() as processed_at
from {{ source('bronze', 'employees') }}
where employee_id is not null
{% if is_incremental() %}
  and updated_at > (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
{% endif %}
