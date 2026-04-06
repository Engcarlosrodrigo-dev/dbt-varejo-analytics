with produtos as (

    select * from {{ ref('int_produtos_categoria') }}

),

final as (

    select
        produto_id,
        sku,
        nome_produto,
        categoria_id,
        categoria_nome,
        departamento,
        fornecedor_id,
        nome_fornecedor,
        preco_custo,
        preco_venda,
        margem_bruta,
        faixa_preco,
        estoque_atual,

        -- Classificação de situação do estoque
        case
            when estoque_atual < 10     then 'CRITICO'
            when estoque_atual < 50     then 'BAIXO'
            when estoque_atual < 500    then 'ADEQUADO'
            else                             'ALTO'
        end                                             as status_estoque,

        total_unidades_vendidas,
        receita_total,
        total_transacoes,
        rank_receita_categoria,

        -- Ranking geral por receita no catálogo
        rank() over (
            order by receita_total desc
        )                                               as rank_receita_geral,

        -- Participação percentual na receita total do catálogo
        round(
            receita_total / nullif(sum(receita_total) over (), 0) * 100,
            4
        )                                               as participacao_receita_pct,

        -- Receita por unidade em estoque (indicador de giro)
        case
            when estoque_atual > 0
            then round(receita_total / estoque_atual, 2)
            else null
        end                                             as receita_por_unidade_estoque,

        ativo

    from produtos

)

select * from final
order by receita_total desc
