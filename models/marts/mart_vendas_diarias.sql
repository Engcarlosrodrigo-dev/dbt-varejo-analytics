with vendas as (

    select * from {{ ref('int_vendas_enriquecidas') }}

),

-- Métricas de vendas concluídas por dia e loja
concluidas as (

    select
        data_venda,
        loja_id,
        nome_loja,
        tipo_loja,
        estado_loja,

        count(venda_id)                     as total_vendas,
        sum(quantidade_itens)               as total_itens_vendidos,
        sum(valor_bruto_total)              as receita_bruta,
        sum(valor_desconto_total)           as total_descontos,
        sum(valor_liquido_total)            as receita_liquida,
        round(avg(valor_liquido_total), 2)  as ticket_medio

    from vendas
    where status_venda = 'CONCLUIDA'
    group by
        data_venda,
        loja_id,
        nome_loja,
        tipo_loja,
        estado_loja

),

-- Contagem de cancelamentos por dia e loja
cancelamentos as (

    select
        data_venda,
        loja_id,
        count(venda_id)     as total_cancelamentos

    from vendas
    where status_venda = 'CANCELADA'
    group by data_venda, loja_id

),

-- Contagem de devoluções por dia e loja
devolucoes as (

    select
        data_venda,
        loja_id,
        count(venda_id)     as total_devolucoes

    from vendas
    where status_venda = 'DEVOLVIDA'
    group by data_venda, loja_id

),

-- Unir tudo
final as (

    select
        c.data_venda,
        c.loja_id,
        c.nome_loja,
        c.tipo_loja,
        c.estado_loja,
        c.total_vendas,
        coalesce(ca.total_cancelamentos, 0) as total_cancelamentos,
        coalesce(d.total_devolucoes, 0)     as total_devolucoes,
        c.receita_bruta,
        c.total_descontos,
        c.receita_liquida,
        c.ticket_medio,
        c.total_itens_vendidos,

        -- Taxa de cancelamento do dia
        round(
            coalesce(ca.total_cancelamentos, 0)::decimal
            / nullif(c.total_vendas + coalesce(ca.total_cancelamentos, 0)
                     + coalesce(d.total_devolucoes, 0), 0) * 100,
            2
        )                                   as taxa_cancelamento_pct

    from concluidas c
    left join cancelamentos ca
        on c.data_venda = ca.data_venda and c.loja_id = ca.loja_id
    left join devolucoes d
        on c.data_venda = d.data_venda and c.loja_id = d.loja_id

)

select * from final
order by data_venda desc, receita_liquida desc
