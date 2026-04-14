{{
    config(
        materialized = 'table',
        schema       = 'warehouse'
    )
}}

/*
  dim_customer — SCD Type 1 (latest record wins).
  customer_id is used as the natural surrogate key — stable across runs.
*/
SELECT
    customer_id                             AS customer_key,
    customer_id,
    customer_unique_id,
    city,
    state,
    CURRENT_TIMESTAMP                       AS _loaded_at
FROM {{ source('silver', 'customers') }}
