{% macro drop_staging_objects(schema_name) %}
  {% set sql %}
    begin
      for r in (select table_name from information_schema.tables where table_schema = upper('{{ schema_name }}') and table_name ilike 'stg_%') do
        execute immediate 'drop view if exists {{ schema_name }}.' || r.table_name;
      end for;
    end;
  {% endset %}
  {% do run_query(sql) %}
  {{ log("✅ Dropped all staging objects in schema " ~ schema_name, info=True) }}
{% endmacro %}
