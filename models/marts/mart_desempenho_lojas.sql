with vendas as (

    select * from {{ ref('int_vendas_enriquecidas') }}
    where status_venda = 'CONCLUIDA'

),

lojas as (

    select * from {{ ref('stg_lojas') }}

),

-- Métricas por loja
metricas_lojas as (

    select
        loja_id,
        count(venda_id)                         as total_vendas,
        sum(valor_liquido_total)                as receita_total,
        round(avg(valor_liquido_total), 2)      as ticket_medio,
        count(distinct cliente_id)              as total_clientes_unicos,
        min(data_venda)                         as primeira_venda,
        max(data_venda)                         as ultima_venda

    from vendas
    group by loja_id

),

-- Média da rede (para benchmarking)
media_rede as (

    select
        round(avg(receita_total), 2)            as receita_media_rede,
        round(avg(ticket_medio), 2)             as ticket_medio_rede

    from metricas_lojas

),

final as (

    select
        l.loja_id,
        l.nome_loja,
        l.tipo_loja,
        l.cidade,
        l.estado,
        l.data_abertura,
        l.anos_operacao,
        l.ativa,

        coalesce(m.total_vendas, 0)             as total_vendas,
        coalesce(m.receita_total, 0)            as receita_total,
        coalesce(m.ticket_medio, 0)             as ticket_medio,
        coalesce(m.total_clientes_unicos, 0)    as total_clientes_unicos,
        m.primeira_venda,
        m.ultima_venda,

        -- Benchmarking vs média da rede
        r.receita_media_rede,
        r.ticket_medio_rede,

        round(
            (coalesce(m.receita_total, 0) - r.receita_media_rede)
            / nullif(r.receita_media_rede, 0) * 100,
            2
        )                                       as variacao_vs_media_pct,

        -- Ranking geral por receita
        rank() over (
            order by coalesce(m.receita_total, 0) desc
        )                                       as rank_receita,

        -- Ranking dentro do tipo de loja (canal)
        rank() over (
            partition by l.tipo_loja
            order by coalesce(m.receita_total, 0) desc
        )                                       as rank_receita_canal,

        -- Receita por ano de operação (maturidade)
        case
            when l.anos_operacao > 0
            then round(coalesce(m.receita_total, 0) / l.anos_operacao, 2)
            else coalesce(m.receita_total, 0)
        end                                     as receita_por_ano_operacao

    from lojas l
    left join metricas_lojas m  on l.loja_id = m.loja_id
    cross join media_rede r

)

select * from final
order by receita_total desc
