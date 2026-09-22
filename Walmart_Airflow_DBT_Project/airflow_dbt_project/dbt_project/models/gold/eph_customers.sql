{{ config(materialized='ephemeral', tags=['gold_ephemeral']) }}

-- Lightweight CTE-only staging: dedupes to one row per customer before
-- being fed into the dim_customers snapshot. No physical table/view written.
select distinct
    customer_id,
    customer_first_name as first_name,
    customer_last_name  as last_name,
    current_timestamp()  as updated_at
from {{ ref('obt') }}
