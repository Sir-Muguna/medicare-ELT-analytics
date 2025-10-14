{% macro generate_schema_name(custom_schema_name, node) -%}
    {# Explicitly set schema logic based on tags #}
    {% if 'staging' in node.tags %}
        {{ 'staging' }}
    {% elif 'marts' in node.tags %}
        {{ 'marts' }}
    {% else %}
        {{ target.schema }}
    {% endif %}
{%- endmacro %}
