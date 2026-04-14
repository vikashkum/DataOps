{{
    config(
        materialized = 'table',
        schema       = 'warehouse'
    )
}}

/*
  dim_seller — SCD Type 1.
  seller_id is used as the natural surrogate key.
*/
SELECT
    seller_id                               AS seller_key,
    seller_id,
    city,
    state,
    CURRENT_TIMESTAMP                       AS _loaded_at
FROM {{ source('silver', 'sellers') }}
