with source as (

    select * from {{ source('raw_varejo', 'fornecedores') }}

),

renomeado as (

    select
        fornecedor_id,
        upper(trim(nome_fornecedor))                    as nome_fornecedor,

        -- Formatar CNPJ: XX.XXX.XXX/XXXX-XX
        case
            when length(replace(replace(replace(cnpj, '.', ''), '/', ''), '-', '')) = 14
            then
                substring(replace(replace(replace(cnpj, '.', ''), '/', ''), '-', ''), 1, 2) || '.' ||
                substring(replace(replace(replace(cnpj, '.', ''), '/', ''), '-', ''), 3, 3) || '.' ||
                substring(replace(replace(replace(cnpj, '.', ''), '/', ''), '-', ''), 6, 3) || '/' ||
                substring(replace(replace(replace(cnpj, '.', ''), '/', ''), '-', ''), 9, 4) || '-' ||
                substring(replace(replace(replace(cnpj, '.', ''), '/', ''), '-', ''), 13, 2)
            else cnpj
        end                                             as cnpj_mascarado,

        upper(trim(cidade))                             as cidade,
        upper(trim(estado))                             as estado,
        cast(ativo as boolean)                          as ativo

    from source

),

filtrado as (

    select *
    from renomeado
    where fornecedor_id is not null
      and nome_fornecedor is not null

)

select * from filtrado
