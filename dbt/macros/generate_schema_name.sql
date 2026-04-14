-- Override dbt's default schema naming so models land in exactly
-- the schema declared in dbt_project.yml (+schema: warehouse),
-- not in <target_schema>_warehouse.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
