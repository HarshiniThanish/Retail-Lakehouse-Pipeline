{% snapshot dim_stores %}
{{
    config(
        target_schema='gold',
        unique_key='store_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
select
    store_id,
    store_name,
    city,
    state,
    region,
    store_type,
    updated_at
from {{ ref('stg_stores') }}
{% endsnapshot %}
