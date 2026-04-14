{{
    config(
        materialized = 'table',
        schema       = 'warehouse'
    )
}}

/*
  dim_product — SCD Type 1.
  product_id is used as the natural surrogate key.
*/
SELECT
    product_id                              AS product_key,
    product_id,
    category,
    weight_g,
    CURRENT_TIMESTAMP                       AS _loaded_at
FROM {{ source('silver', 'products') }}
