with vendas as (

    select * from {{ ref('stg_vendas') }}

),

itens as (

    select * from {{ ref('stg_itens_venda') }}

),

clientes as (

    select * from {{ ref('stg_clientes') }}

),

lojas as (

    select * from {{ ref('stg_lojas') }}

),

-- Agregar itens por venda
itens_agregados as (

    select
        venda_id,
        count(item_id)                  as quantidade_itens,
        sum(valor_bruto_item)           as valor_bruto_total,
        sum(valor_desconto_item)        as valor_desconto_itens,
        sum(valor_liquido_item)         as valor_liquido_itens

    from itens
    group by venda_id

),

-- Unir vendas com itens agregados
vendas_com_itens as (

    select
        v.venda_id,
        v.cliente_id,
        v.loja_id,
        v.forma_pagamento_id,
        v.data_venda,
        v.hora_venda,
        v.status_venda,
        v.valor_desconto                                        as valor_desconto_cabecalho,
        v.created_at,
        v.updated_at,

        coalesce(i.quantidade_itens, 0)                         as quantidade_itens,
        coalesce(i.valor_bruto_total, 0)                        as valor_bruto_total,
        coalesce(i.valor_desconto_itens, 0)
            + coalesce(v.valor_desconto, 0)                     as valor_desconto_total,
        coalesce(i.valor_liquido_itens, 0)
            - coalesce(v.valor_desconto, 0)                     as valor_liquido_total

    from vendas v
    left join itens_agregados i on v.venda_id = i.venda_id

),

-- Enriquecer com dados de loja
com_loja as (

    select
        vc.*,
        l.nome_loja,
        l.tipo_loja,
        l.cidade                        as cidade_loja,
        l.estado                        as estado_loja,
        l.anos_operacao

    from vendas_com_itens vc
    left join lojas l on vc.loja_id = l.loja_id

),

-- Enriquecer com dados de cliente
com_cliente as (

    select
        cl.*,
        c.nome_completo                 as nome_cliente,
        c.email                         as email_cliente,
        c.cidade                        as cidade_cliente,
        c.estado                        as estado_cliente,
        c.genero                        as genero_cliente,
        c.idade                         as idade_cliente,
        c.data_cadastro                 as data_cadastro_cliente

    from com_loja cl
    left join clientes c on cl.cliente_id = c.cliente_id

)

select * from com_cliente
