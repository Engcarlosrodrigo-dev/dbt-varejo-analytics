with source as (

    select * from {{ source('raw_varejo', 'produtos') }}

),

renomeado as (

    select
        produto_id,
        upper(trim(sku))                                as sku,
        trim(nome_produto)                              as nome_produto,
        categoria_id,
        fornecedor_id,

        cast(preco_custo as decimal(12, 2))             as preco_custo,
        cast(preco_venda as decimal(12, 2))             as preco_venda,

        -- Margem bruta percentual
        case
            when preco_venda > 0
            then round(
                (cast(preco_venda as decimal(12, 4)) - cast(preco_custo as decimal(12, 4)))
                / cast(preco_venda as decimal(12, 4)) * 100,
                2
            )
            else 0
        end                                             as margem_bruta,

        -- Classificação por faixa de preço
        case
            when cast(preco_venda as decimal) < 50    then 'ECONOMICO'
            when cast(preco_venda as decimal) < 500   then 'MEDIO'
            else 'PREMIUM'
        end                                             as faixa_preco,

        cast(estoque_atual as integer)                  as estoque_atual,
        cast(ativo as boolean)                          as ativo

    from source

),

filtrado as (

    select *
    from renomeado
    where produto_id is not null
      and sku is not null
      and nome_produto is not null

)

select * from filtrado
