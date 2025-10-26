{% macro generate_schema_name(custom_schema_name, node) -%}
    {# 
      Use the custom schema if provided,
      otherwise fallback to the target.schema (from profiles.yml)
    #}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
