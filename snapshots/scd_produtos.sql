{% snapshot scd_produtos %}
    {{
        config(
            target_schema = 'snapshots',
            unique_key    = 'produto_id',
            strategy      = 'check',
            check_cols    = ['preco_venda', 'preco_custo', 'estoque_atual', 'ativo'],
        )
    }}

    select
        produto_id,
        sku,
        nome_produto,
        categoria_id,
        fornecedor_id,
        preco_custo,
        preco_venda,
        estoque_atual,
        ativo,
        current_timestamp as updated_at

    from {{ ref('stg_produtos') }}

{% endsnapshot %}
