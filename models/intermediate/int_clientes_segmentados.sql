with clientes as (

    select * from {{ ref('stg_clientes') }}

),

vendas as (

    select * from {{ ref('int_vendas_enriquecidas') }}
    where status_venda = 'CONCLUIDA'

),

-- Métricas agregadas por cliente
metricas_cliente as (

    select
        cliente_id,
        count(venda_id)                                     as total_compras,
        min(data_venda)                                     as primeira_compra,
        max(data_venda)                                     as ultima_compra,
        {{ datediff('max(data_venda)', 'current_date', 'day') }} as dias_desde_ultima_compra,
        round(avg(valor_liquido_total), 2)                  as ticket_medio,
        round(sum(valor_liquido_total), 2)                  as valor_total_gasto

    from vendas
    group by cliente_id

),

-- Calcular scores RFM via ntile (quintis)
scores_rfm as (

    select
        cliente_id,
        total_compras,
        primeira_compra,
        ultima_compra,
        dias_desde_ultima_compra,
        ticket_medio,
        valor_total_gasto,

        -- Score de recência: menor número de dias = score maior
        ntile(5) over (order by dias_desde_ultima_compra desc)  as score_recencia,
        -- Score de frequência: mais compras = score maior
        ntile(5) over (order by total_compras asc)              as score_frequencia,
        -- Score monetário: maior gasto = score maior
        ntile(5) over (order by valor_total_gasto asc)          as score_monetario

    from metricas_cliente

),

-- Classificar segmento RFM
segmentacao as (

    select
        *,
        score_recencia + score_frequencia + score_monetario     as rfm_total,

        case
            when score_recencia >= 4 and score_frequencia >= 4
                and score_monetario >= 4                        then 'CAMPIAO'
            when score_recencia >= 3 and score_frequencia >= 3
                and score_monetario >= 3                        then 'LEAL'
            when score_recencia >= 3 and score_frequencia <= 2  then 'POTENCIAL'
            when score_recencia <= 2 and score_frequencia >= 3  then 'RISCO'
            else                                                     'PERDIDO'
        end                                                     as segmento_rfm

    from scores_rfm

),

-- Unir com dados cadastrais do cliente
final as (

    select
        c.cliente_id,
        c.nome_completo,
        c.email,
        c.genero,
        c.idade,
        c.cidade,
        c.estado,
        c.data_cadastro,
        c.ativo,

        coalesce(s.total_compras, 0)                            as total_compras,
        s.primeira_compra,
        s.ultima_compra,
        s.dias_desde_ultima_compra,
        coalesce(s.ticket_medio, 0)                             as ticket_medio,
        coalesce(s.valor_total_gasto, 0)                        as valor_total_gasto,
        s.score_recencia,
        s.score_frequencia,
        s.score_monetario,
        s.rfm_total,
        s.segmento_rfm

    from clientes c
    left join segmentacao s on c.cliente_id = s.cliente_id

)

select * from final
