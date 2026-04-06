# dbt Varejo Analytics

Projeto dbt de estudo de caso completo para análise de dados de varejo.
Implementa as camadas **staging → intermediate → marts** com dados de
vendas, clientes, produtos e lojas.

## Estrutura do Projeto

```
dbt-varejo-analytics/
├── models/
│   ├── staging/              # Limpeza e padronização das fontes
│   │   ├── _sources.yml      # Definição das fontes raw
│   │   ├── _staging.yml      # Testes e documentação dos modelos
│   │   ├── stg_vendas.sql
│   │   ├── stg_itens_venda.sql
│   │   ├── stg_clientes.sql
│   │   ├── stg_produtos.sql
│   │   ├── stg_lojas.sql
│   │   └── stg_fornecedores.sql
│   ├── intermediate/         # Lógica de negócio e enriquecimento
│   │   ├── _intermediate.yml
│   │   ├── int_vendas_enriquecidas.sql
│   │   ├── int_clientes_segmentados.sql
│   │   └── int_produtos_categoria.sql
│   └── marts/                # Tabelas analíticas finais
│       ├── _marts.yml
│       ├── mart_vendas_diarias.sql
│       ├── mart_desempenho_produtos.sql
│       ├── mart_clientes_rfm.sql
│       └── mart_desempenho_lojas.sql
├── seeds/                    # Dados de referência estáticos
│   ├── categorias_produto.csv
│   ├── regioes.csv
│   └── formas_pagamento.csv
├── snapshots/                # SCD Tipo 2 para produtos
│   └── scd_produtos.sql
├── macros/                   # Funções reutilizáveis Jinja/SQL
│   ├── datediff.sql
│   ├── generate_schema_name.sql
│   └── tests_genericos.sql
├── analyses/                 # Queries exploratórias ad-hoc
│   ├── analise_sazonalidade.sql
│   └── analise_cohort_clientes.sql
├── dbt_project.yml
├── packages.yml
└── profiles.yml
```

## Camadas de Dados

### Staging (`models/staging/`)
- Materialização: **view**
- Responsabilidade: renomear colunas, aplicar tipos corretos, filtrar nulos críticos
- Convenção: prefixo `stg_`

### Intermediate (`models/intermediate/`)
- Materialização: **ephemeral** (sem tabela física)
- Responsabilidade: joins entre entidades, cálculos de negócio, segmentação
- Convenção: prefixo `int_`

### Marts (`models/marts/`)
- Materialização: **table**
- Responsabilidade: tabelas prontas para consumo por BI e analistas
- Convenção: prefixo `mart_`

## Modelos Principais

| Modelo | Camada | Descrição |
|---|---|---|
| `stg_vendas` | Staging | Ordens de venda padronizadas |
| `stg_itens_venda` | Staging | Itens com valor bruto/líquido calculados |
| `stg_clientes` | Staging | Clientes com idade e CPF mascarado |
| `stg_produtos` | Staging | Produtos com margem e faixa de preço |
| `int_vendas_enriquecidas` | Intermediate | Vendas + itens + cliente + loja |
| `int_clientes_segmentados` | Intermediate | Clientes com métricas RFM |
| `int_produtos_categoria` | Intermediate | Produtos + categoria + métricas de venda |
| `mart_vendas_diarias` | Mart | KPIs diários por loja |
| `mart_desempenho_produtos` | Mart | Performance e giro de produtos |
| `mart_clientes_rfm` | Mart | Segmentação e LTV de clientes |
| `mart_desempenho_lojas` | Mart | Ranking e benchmark de lojas |

## Pré-requisitos

```bash
pip install dbt-duckdb
```

## Configuração

O `profiles.yml` já está configurado para DuckDB (banco local, sem infraestrutura):

```yaml
varejo_analytics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "varejo_analytics.duckdb"
      threads: 4
```

## Como Executar

```bash
# Instalar pacotes dbt
dbt deps

# Carregar seeds (dados de referência)
dbt seed

# Executar todos os modelos
dbt run

# Executar apenas uma camada
dbt run --select staging
dbt run --select intermediate
dbt run --select marts

# Executar testes de qualidade
dbt test

# Gerar documentação
dbt docs generate
dbt docs serve

# Executar snapshots (SCD Tipo 2)
dbt snapshot
```

## Análises Ad-hoc

```bash
# Compilar uma análise para visualizar o SQL gerado
dbt compile --select analyses/analise_sazonalidade
dbt compile --select analyses/analise_cohort_clientes
```

## Variáveis do Projeto

| Variável | Padrão | Descrição |
|---|---|---|
| `data_inicio` | `2023-01-01` | Início do período de análise |
| `data_fim` | `2024-12-31` | Fim do período de análise |
| `moeda` | `BRL` | Moeda utilizada |

Override via CLI: `dbt run --vars '{"data_inicio": "2024-01-01"}'`
