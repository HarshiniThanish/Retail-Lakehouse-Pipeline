{#-
    generate_obt(config)
    ---------------------------------------------------------------------
    Builds a SELECT ... FROM ... JOIN ... statement entirely from a
    metadata dictionary, instead of hand-writing static SQL joins.

    `config` shape:
    {
      "base": {"ref": "stg_orders", "alias": "o"},
      "joins": [
        {"ref": "stg_customers", "alias": "c", "type": "left",
         "on": "o.customer_id = c.customer_id"},
        {"ref": "stg_stores",    "alias": "s", "type": "left",
         "on": "o.store_id = s.store_id"},
        {"ref": "stg_employees", "alias": "e", "type": "left",
         "on": "o.employee_id = e.employee_id"},
        {"ref": "stg_order_items", "alias": "oi", "type": "inner",
         "on": "o.order_id = oi.order_id"},
        {"ref": "stg_products",  "alias": "p", "type": "left",
         "on": "oi.product_id = p.product_id"}
      ],
      "columns": [
        {"expr": "o.order_id",        "alias": "order_id"},
        {"expr": "o.order_date",      "alias": "order_date"},
        {"expr": "o.order_status",    "alias": "order_status"},
        {"expr": "c.customer_id",     "alias": "customer_id"},
        {"expr": "c.first_name",      "alias": "customer_first_name"},
        {"expr": "c.last_name",       "alias": "customer_last_name"},
        {"expr": "s.store_id",        "alias": "store_id"},
        {"expr": "s.store_name",      "alias": "store_name"},
        {"expr": "e.employee_id",     "alias": "employee_id"},
        {"expr": "oi.order_item_id",  "alias": "order_item_id"},
        {"expr": "oi.quantity",       "alias": "quantity"},
        {"expr": "oi.unit_price",     "alias": "unit_price"},
        {"expr": "oi.line_amount",    "alias": "line_amount"},
        {"expr": "p.product_id",      "alias": "product_id"},
        {"expr": "p.product_name",    "alias": "product_name"},
        {"expr": "p.category",        "alias": "product_category"}
      ]
    }

    No table name, column list, alias, or JOIN clause is hardcoded in the
    calling model (obt.sql) — everything comes from this dictionary, which
    typically lives in a var (see models/silver_b/obt_config.yml docs and
    dbt_project.yml `vars:` block).
    --------------------------------------------------------------------- #}
{% macro generate_obt(config) %}

{%- set base = config['base'] -%}
{%- set joins = config.get('joins', []) -%}
{%- set columns = config['columns'] -%}

select
{%- for col in columns %}
    {{ col['expr'] }} as {{ col['alias'] }}{{ "," if not loop.last }}
{%- endfor %}
from {{ ref(base['ref']) }} as {{ base['alias'] }}
{%- for j in joins %}
{{ j.get('type', 'left') }} join {{ ref(j['ref']) }} as {{ j['alias'] }}
    on {{ j['on'] }}
{%- endfor %}

{% endmacro %}
