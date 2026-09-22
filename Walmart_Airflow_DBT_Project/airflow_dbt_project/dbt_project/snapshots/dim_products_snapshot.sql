{% snapshot dim_products %}
{{
    config(
        target_schema='gold',
        unique_key='product_id',
        strategy='timestamp',
        updated_at='updated_at',
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
    updated_at
from {{ ref('stg_products') }}
{% endsnapshot %}
