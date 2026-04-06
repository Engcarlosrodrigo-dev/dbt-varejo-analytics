with source as (

    select * from {{ source('raw_varejo', 'vendas') }}

),

renomeado as (

    select
        venda_id,
        cliente_id,
        loja_id,
        forma_pagamento_id,

        -- Separar data e hora da venda
        cast(data_venda as date)                        as data_venda,
        cast(data_venda as time)                        as hora_venda,

        -- Normalizar status
        upper(trim(status_venda))                       as status_venda,

        -- Garantir que desconto seja >= 0
        coalesce(valor_desconto, 0)                     as valor_desconto,

        -- Timestamps de auditoria
        cast(created_at as timestamp)                   as created_at,
        cast(updated_at as timestamp)                   as updated_at

    from source

),

filtrado as (

    select *
    from renomeado
    -- Excluir registros sem chaves obrigatórias
    where venda_id is not null
      and cliente_id is not null
      and loja_id is not null
      and data_venda is not null

)

select * from filtrado
