{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Sobrescreve o comportamento padrão do dbt para geração de schema.
        Em dev: usa apenas o schema customizado (sem prefixo de usuário).
        Em prod: usa o schema definido no dbt_project.yml.
    #}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
