{% macro create_staging_model(source_schema, source_table) %}
  with source as (
    select
      *
    from {{ source(source_schema, source_table) }}
  )
  select
  *,
  current_timestamp() as ingestion_timestamp
  from source
{% endmacro %}
