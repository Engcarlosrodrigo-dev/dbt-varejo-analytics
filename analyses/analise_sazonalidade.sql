-- Análise exploratória de sazonalidade de vendas
-- Uso: dbt compile --select analyses/analise_sazonalidade

with vendas as (

    select
        data_venda,
        extract(year from data_venda)   as ano,
        extract(month from data_venda)  as mes,
        extract(dow from data_venda)    as dia_semana,  -- 0=Dom, 6=Sab
        tipo_loja,
        estado_loja,
        valor_liquido_total,
        quantidade_itens

    from {{ ref('int_vendas_enriquecidas') }}
    where status_venda = 'CONCLUIDA'

),

-- Receita por mês e ano
sazonalidade_mensal as (

    select
        ano,
        mes,
        count(*)                        as total_vendas,
        sum(valor_liquido_total)        as receita_liquida,
        round(avg(valor_liquido_total), 2) as ticket_medio

    from vendas
    group by ano, mes
    order by ano, mes

),

-- Receita por dia da semana
sazonalidade_semanal as (

    select
        dia_semana,
        case dia_semana
            when 0 then 'Domingo'
            when 1 then 'Segunda'
            when 2 then 'Terça'
            when 3 then 'Quarta'
            when 4 then 'Quinta'
            when 5 then 'Sexta'
            when 6 then 'Sábado'
        end                             as nome_dia,
        count(*)                        as total_vendas,
        round(avg(valor_liquido_total), 2) as ticket_medio

    from vendas
    group by dia_semana
    order by dia_semana

)

select * from sazonalidade_mensal
