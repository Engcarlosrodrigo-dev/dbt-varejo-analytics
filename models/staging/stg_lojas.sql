with source as (

    select * from {{ source('raw_varejo', 'lojas') }}

),

renomeado as (

    select
        loja_id,
        upper(trim(nome_loja))                          as nome_loja,
        upper(trim(tipo_loja))                          as tipo_loja,
        upper(trim(cidade))                             as cidade,
        upper(trim(estado))                             as estado,
        cast(data_abertura as date)                     as data_abertura,

        -- Anos em operação
        {{ datediff('data_abertura', 'current_date', 'year') }} as anos_operacao,

        cast(ativa as boolean)                          as ativa

    from source

),

filtrado as (

    select *
    from renomeado
    where loja_id is not null
      and nome_loja is not null

)

select * from filtrado
