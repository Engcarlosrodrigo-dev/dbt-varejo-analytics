with clientes_segmentados as (

    select * from {{ ref('int_clientes_segmentados') }}

),

final as (

    select
        cliente_id,
        nome_completo,
        email,
        genero,
        idade,

        -- Faixa etária
        case
            when idade between 18 and 25    then '18-25'
            when idade between 26 and 35    then '26-35'
            when idade between 36 and 45    then '36-45'
            when idade between 46 and 60    then '46-60'
            when idade > 60                 then '60+'
            else                                 'NAO INFORMADO'
        end                                             as faixa_etaria,

        cidade,
        estado,
        data_cadastro,
        ativo,

        -- Métricas de compra
        total_compras,
        primeira_compra,
        ultima_compra,
        dias_desde_ultima_compra,
        ticket_medio,
        valor_total_gasto,

        -- Scores e segmento RFM
        score_recencia,
        score_frequencia,
        score_monetario,
        rfm_total,
        segmento_rfm,

        -- LTV simples: ticket médio * frequência anual estimada * 3 anos
        case
            when total_compras > 0 and primeira_compra is not null
            then round(
                ticket_medio
                * (total_compras::decimal
                   / nullif({{ datediff('primeira_compra', 'current_date', 'month') }}, 0)
                   * 12)
                * 3,
                2
            )
            else 0
        end                                             as valor_lifetime,

        -- Flag de cliente em risco de churn
        case
            when dias_desde_ultima_compra > 180 and total_compras > 1 then true
            else false
        end                                             as flag_risco_churn

    from clientes_segmentados

)

select * from final
order by valor_total_gasto desc
