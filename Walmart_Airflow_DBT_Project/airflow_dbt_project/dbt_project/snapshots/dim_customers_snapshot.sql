{% snapshot dim_customers %}
{{
    config(
        target_schema='gold',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
select * from {{ ref('eph_customers') }}
{% endsnapshot %}
