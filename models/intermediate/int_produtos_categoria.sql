with produtos as (

    select * from {{ ref('stg_produtos') }}

),

fornecedores as (

    select * from {{ ref('stg_fornecedores') }}

),

categorias as (

    select * from {{ ref('categorias_produto') }}

),

itens as (

    select * from {{ ref('stg_itens_venda') }}

),

vendas as (

    -- Apenas vendas concluídas para métricas de produto
    select venda_id from {{ ref('stg_vendas') }}
    where status_venda = 'CONCLUIDA'

),

-- Métricas de venda por produto
metricas_produto as (

    select
        i.produto_id,
        sum(i.quantidade)               as total_unidades_vendidas,
        sum(i.valor_liquido_item)       as receita_total,
        count(distinct i.venda_id)      as total_transacoes

    from itens i
    inner join vendas v on i.venda_id = v.venda_id
    group by i.produto_id

),

-- Enriquecer produtos
enriquecido as (

    select
        p.produto_id,
        p.sku,
        p.nome_produto,
        p.categoria_id,
        c.categoria_nome,
        c.departamento,
        p.fornecedor_id,
        f.nome_fornecedor,
        p.preco_custo,
        p.preco_venda,
        p.margem_bruta,
        p.faixa_preco,
        p.estoque_atual,
        p.ativo,

        coalesce(m.total_unidades_vendidas, 0)      as total_unidades_vendidas,
        coalesce(m.receita_total, 0)                as receita_total,
        coalesce(m.total_transacoes, 0)             as total_transacoes

    from produtos p
    left join categorias c      on p.categoria_id = c.categoria_id
    left join fornecedores f    on p.fornecedor_id = f.fornecedor_id
    left join metricas_produto m on p.produto_id = m.produto_id

),

-- Ranking dentro de cada categoria
final as (

    select
        *,
        rank() over (
            partition by categoria_id
            order by receita_total desc
        )                                           as rank_receita_categoria

    from enriquecido

)

select * from final
