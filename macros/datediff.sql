{% macro datediff(start_date, end_date, datepart) %}
    {#
        Macro de compatibilidade para calcular diferença entre datas.
        Suporta DuckDB e PostgreSQL.

        Args:
            start_date: data inicial (coluna ou expressão)
            end_date:   data final (coluna ou expressão)
            datepart:   'day', 'month', 'year'
    #}
    {% if target.type == 'duckdb' %}
        date_diff('{{ datepart }}', cast({{ start_date }} as date), cast({{ end_date }} as date))
    {% elif target.type in ('postgres', 'redshift') %}
        {% if datepart == 'day' %}
            (cast({{ end_date }} as date) - cast({{ start_date }} as date))
        {% elif datepart == 'month' %}
            (
                (extract(year from cast({{ end_date }} as date))
                 - extract(year from cast({{ start_date }} as date))) * 12
                + extract(month from cast({{ end_date }} as date))
                - extract(month from cast({{ start_date }} as date))
            )
        {% elif datepart == 'year' %}
            (
                extract(year from cast({{ end_date }} as date))
                - extract(year from cast({{ start_date }} as date))
            )
        {% endif %}
    {% elif target.type == 'bigquery' %}
        date_diff(cast({{ end_date }} as date), cast({{ start_date }} as date), {{ datepart }})
    {% else %}
        datediff('{{ datepart }}', cast({{ start_date }} as date), cast({{ end_date }} as date))
    {% endif %}
{% endmacro %}
