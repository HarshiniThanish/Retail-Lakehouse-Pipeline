{% snapshot dim_employees %}
{{
    config(
        target_schema='gold',
        unique_key='employee_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
select
    employee_id,
    first_name,
    last_name,
    store_id,
    role,
    is_active,
    updated_at
from {{ ref('stg_employees') }}
{% endsnapshot %}
