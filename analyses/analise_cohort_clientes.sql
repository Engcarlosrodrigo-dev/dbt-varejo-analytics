-- Análise de coorte de clientes por mês de primeira compra
-- Mede retenção mensal de cada coorte

with vendas as (

    select
        cliente_id,
        data_venda,
        valor_liquido_total

    from {{ ref('int_vendas_enriquecidas') }}
    where status_venda = 'CONCLUIDA'

),

-- Determinar mês de primeira compra de cada cliente
primeira_compra as (

    select
        cliente_id,
        min(data_venda)                             as data_primeira_compra,
        date_trunc('month', min(data_venda))        as cohort_mes

    from vendas
    group by cliente_id

),

-- Unir vendas com cohort
vendas_com_cohort as (

    select
        v.cliente_id,
        v.data_venda,
        v.valor_liquido_total,
        p.cohort_mes,

        -- Número do período (meses desde a primeira compra)
        {{ datediff('p.cohort_mes', "date_trunc('month', v.data_venda)", 'month') }}
            as periodo_cohort

    from vendas v
    inner join primeira_compra p on v.cliente_id = p.cliente_id

),

-- Agregar por cohort e período
cohort_data as (

    select
        cohort_mes,
        periodo_cohort,
        count(distinct cliente_id)      as clientes_ativos,
        sum(valor_liquido_total)        as receita_cohort

    from vendas_com_cohort
    group by cohort_mes, periodo_cohort

),

-- Adicionar tamanho do cohort (período 0)
tamanho_cohort as (

    select
        cohort_mes,
        clientes_ativos                 as tamanho_cohort

    from cohort_data
    where periodo_cohort = 0

)

select
    c.cohort_mes,
    c.periodo_cohort,
    t.tamanho_cohort,
    c.clientes_ativos,
    round(c.clientes_ativos::decimal / t.tamanho_cohort * 100, 2)  as taxa_retencao_pct,
    c.receita_cohort

from cohort_data c
inner join tamanho_cohort t on c.cohort_mes = t.cohort_mes
order by c.cohort_mes, c.periodo_cohort
