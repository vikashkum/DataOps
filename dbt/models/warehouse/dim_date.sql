{{
    config(
        materialized = 'table',
        schema       = 'warehouse'
    )
}}

/*
  dim_date — generated entirely in SQL using PostgreSQL generate_series.
  Covers 2020-01-01 to 2026-12-31.  Extend end_date as needed.
*/
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT             AS date_key,
    d::DATE                                 AS full_date,
    EXTRACT(DAY     FROM d)::SMALLINT       AS day,
    EXTRACT(MONTH   FROM d)::SMALLINT       AS month,
    EXTRACT(YEAR    FROM d)::SMALLINT       AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT       AS quarter,
    EXTRACT(DOW     FROM d)::SMALLINT       AS day_of_week,
    TRIM(TO_CHAR(d, 'Day'))                 AS day_name,
    TRIM(TO_CHAR(d, 'Month'))               AS month_name,
    EXTRACT(DOW FROM d) IN (0, 6)           AS is_weekend
FROM
    generate_series(
        '2020-01-01'::DATE,
        '2026-12-31'::DATE,
        '1 day'::INTERVAL
    ) AS g(d)
