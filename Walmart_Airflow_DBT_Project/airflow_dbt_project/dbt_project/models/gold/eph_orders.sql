{{ config(materialized='ephemeral', tags=['gold_ephemeral']) }}

select distinct
    order_id,
    order_date,
    order_status,
    customer_id,
    store_id,
    employee_id,
    current_timestamp() as updated_at
from {{ ref('obt') }}
