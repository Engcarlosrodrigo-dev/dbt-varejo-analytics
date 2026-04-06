with source as (

    select * from {{ source('raw_varejo', 'itens_venda') }}

),

renomeado as (

    select
        item_id,
        venda_id,
        produto_id,

        -- Garantir valores numéricos válidos
        cast(quantidade as integer)                         as quantidade,
        cast(preco_unitario as decimal(12, 2))              as preco_unitario,
        coalesce(cast(valor_desconto_item as decimal(12, 2)), 0) as valor_desconto_item,

        -- Calcular valores derivados
        cast(quantidade as decimal(12, 2))
            * cast(preco_unitario as decimal(12, 2))        as valor_bruto_item,

        (cast(quantidade as decimal(12, 2))
            * cast(preco_unitario as decimal(12, 2)))
            - coalesce(cast(valor_desconto_item as decimal(12, 2)), 0) as valor_liquido_item

    from source

),

filtrado as (

    select *
    from renomeado
    where item_id is not null
      and venda_id is not null
      and produto_id is not null
      and quantidade > 0
      and preco_unitario > 0

)

select * from filtrado
