with source as (

    select * from {{ source('raw_varejo', 'clientes') }}

),

renomeado as (

    select
        cliente_id,

        -- Padronizar texto
        upper(trim(nome_completo))                      as nome_completo,
        lower(trim(email))                              as email,

        -- Mascarar CPF (exibe apenas últimos 2 dígitos)
        '***.***.***-' || right(replace(replace(cpf, '.', ''), '-', ''), 2) as cpf_mascarado,

        cast(data_nascimento as date)                   as data_nascimento,

        -- Calcular idade em anos
        {{ datediff('data_nascimento', 'current_date', 'year') }} as idade,

        -- Normalizar gênero
        case
            when upper(trim(genero)) in ('M', 'MASCULINO') then 'M'
            when upper(trim(genero)) in ('F', 'FEMININO')  then 'F'
            else 'N'
        end                                             as genero,

        upper(trim(cidade))                             as cidade,
        upper(trim(estado))                             as estado,
        cast(data_cadastro as date)                     as data_cadastro,
        cast(ativo as boolean)                          as ativo

    from source

),

filtrado as (

    select *
    from renomeado
    where cliente_id is not null
      and email is not null

)

select * from filtrado
