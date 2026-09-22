{{
    config(
        materialized='table',
        tags=['silver_business']
    )
}}

-- The entire join graph below comes from `var('obt_config')`, defined in
-- dbt_project.yml (or overridden with --vars at run time). Adding a new
-- table to the OBT means editing the dictionary, not this file.
{{ generate_obt(var('obt_config')) }}
