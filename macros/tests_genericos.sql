{% macro test_valor_positivo(model, column_name) %}
    {#
        Teste genérico: verifica que todos os valores de uma coluna são > 0.
        Uso no schema.yml:
            tests:
              - varejo_analytics.valor_positivo
    #}
    select *
    from {{ model }}
    where {{ column_name }} <= 0

{% endmacro %}


{% macro test_sem_espacos_extras(model, column_name) %}
    {#
        Teste genérico: verifica que uma coluna de texto não tem
        espaços no início ou fim.
    #}
    select *
    from {{ model }}
    where {{ column_name }} != trim({{ column_name }})
      and {{ column_name }} is not null

{% endmacro %}


{% macro test_data_no_periodo(model, column_name) %}
    {#
        Teste genérico: verifica que datas estão dentro do período
        configurado nas variáveis do projeto.
    #}
    select *
    from {{ model }}
    where {{ column_name }} < cast('{{ var("data_inicio") }}' as date)
       or {{ column_name }} > cast('{{ var("data_fim") }}' as date)

{% endmacro %}
