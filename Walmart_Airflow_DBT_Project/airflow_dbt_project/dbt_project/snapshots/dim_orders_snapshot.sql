{% snapshot dim_orders %}
{{
    config(
        target_schema='gold',
        unique_key='order_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
select * from {{ ref('eph_orders') }}
{% endsnapshot %}
