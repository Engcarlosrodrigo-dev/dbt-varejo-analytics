# 🏪 dbt Varejo Analytics — Plataforma Analítica para Rede de Varejo

> **Estudo de Caso:** Modernização da camada analítica de uma rede de varejo com 120 lojas e R$ 2,8 bilhões em faturamento anual, eliminando inconsistências de métricas e reduzindo o tempo de geração de relatórios de 6 horas para 12 minutos.

---

## 📌 Contexto do Problema

A **VarejoMax** (nome fictício), uma rede varejista com operações em 5 estados brasileiros, enfrentava uma crise silenciosa de confiança nos dados:

- **Relatórios divergentes:** O time de vendas reportava um ticket médio de R$ 187, enquanto o financeiro apontava R$ 203 — para o mesmo período.
- **Tempo de geração:** Relatórios executivos levavam 6 horas para serem gerados manualmente toda segunda-feira.
- **Silos de dados:** 4 sistemas legados (ERP, CRM, PDV, E-commerce) sem integração, cada um com suas próprias regras de negócio.
- **Sem rastreabilidade:** Impossível saber de onde um número vinha ou qual era sua regra de cálculo.
- **Custo operacional:** 3 analistas dedicados exclusivamente à extração e cruzamento manual de dados.

### 💡 Solução Proposta

Implementação de uma **camada analítica centralizada com dbt**, unificando as fontes de dados em um único modelo semântico confiável, com testes automatizados, documentação viva e deploy contínuo.

---

## 🏗️ Arquitetura da Solução

```
┌─────────────────────────────────────────────────────────────────┐
│                        FONTES DE DADOS                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐   │
│  │   ERP    │  │   CRM    │  │   PDV    │  │  E-commerce  │   │
│  │(Totvs)   │  │(Salesfor)│  │(LINX)    │  │  (VTEX)      │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └──────┬───────┘   │
└───────┼─────────────┼─────────────┼────────────────┼───────────┘
        │             │             │                │
        └─────────────┴──────┬──────┴────────────────┘
                             │  (Airbyte / Fivetran)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    RAW LAYER (BigQuery)                         │
│              Dados brutos — sem transformação                   │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼  dbt run
┌─────────────────────────────────────────────────────────────────┐
│                  STAGING LAYER                                  │
│    Limpeza • Padronização • Renomeação • Cast de tipos          │
│  stg_erp_* | stg_crm_* | stg_pdv_* | stg_ecommerce_*          │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼  dbt run
┌─────────────────────────────────────────────────────────────────┐
│                INTERMEDIATE LAYER                               │
│      Regras de negócio • Joins • Enriquecimento                 │
│  int_vendas_unificadas | int_clientes_360 | int_estoque         │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼  dbt run
┌─────────────────────────────────────────────────────────────────┐
│                    MARTS LAYER                                  │
│         Modelos prontos para consumo por domínio                │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐    │
│  │  Financeiro  │ │  Comercial   │ │     Operacional      │    │
│  │ fct_receita  │ │ fct_vendas   │ │ fct_estoque          │    │
│  │ dim_centros  │ │ fct_metas    │ │ fct_ruptura          │    │
│  │ dim_contas   │ │ dim_clientes │ │ dim_lojas            │    │
│  └──────────────┘ └──────────────┘ └──────────────────────┘    │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│              CAMADA DE CONSUMO                                  │
│    Power BI • Looker Studio • Metabase • APIs internas          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📊 Resultados Alcançados

| Métrica | Antes | Depois | Melhoria |
|---|---|---|---|
| Tempo de geração de relatórios | 6 horas | 12 minutos | **-97%** |
| Inconsistências de métricas | 14 divergências/mês | 0 | **-100%** |
| Modelos documentados | 0% | 100% | **+100%** |
| Cobertura de testes | 0% | 94% | **+94%** |
| Analistas em tarefas manuais | 3 dedicados | 0 | **Realocados** |
| Custo de infraestrutura analítica | R$ 28.000/mês | R$ 9.400/mês | **-66%** |

---

## 🗂️ Estrutura do Projeto

```
dbt-varejo-analytics/
│
├── models/
│   ├── staging/                    # Camada de limpeza por fonte
│   │   ├── erp/
│   │   │   ├── stg_erp_notas_fiscais.sql
│   │   │   ├── stg_erp_produtos.sql
│   │   │   └── stg_erp_fornecedores.sql
│   │   ├── crm/
│   │   │   ├── stg_crm_clientes.sql
│   │   │   └── stg_crm_interacoes.sql
│   │   ├── pdv/
│   │   │   ├── stg_pdv_transacoes.sql
│   │   │   └── stg_pdv_itens.sql
│   │   └── ecommerce/
│   │       ├── stg_ecommerce_pedidos.sql
│   │       └── stg_ecommerce_itens.sql
│   │
│   ├── intermediate/               # Regras de negócio e integrações
│   │   ├── int_vendas_unificadas.sql
│   │   ├── int_clientes_360.sql
│   │   ├── int_estoque_posicao.sql
│   │   └── int_metas_vs_realizado.sql
│   │
│   └── marts/                      # Modelos por domínio de negócio
│       ├── financeiro/
│       │   ├── fct_receita_diaria.sql
│       │   └── dim_centro_de_custo.sql
│       ├── comercial/
│       │   ├── fct_vendas.sql
│       │   ├── fct_metas_vendedores.sql
│       │   ├── dim_clientes.sql
│       │   └── dim_produtos.sql
│       └── operacional/
│           ├── fct_estoque.sql
│           ├── fct_ruptura_estoque.sql
│           └── dim_lojas.sql
│
├── tests/                          # Testes customizados
│   ├── assert_margem_positiva.sql
│   ├── assert_ticket_medio_valido.sql
│   └── assert_estoque_nao_negativo.sql
│
├── macros/                         # Funções reutilizáveis
│   ├── gerar_surrogate_key.sql
│   ├── calcular_ticket_medio.sql
│   └── classificar_cliente_rfm.sql
│
├── seeds/                          # Dados de referência estáticos
│   ├── regioes_brasil.csv
│   ├── categorias_produto.csv
│   └── metas_mensais_2024.csv
│
├── analyses/                       # Análises ad-hoc documentadas
│   ├── analise_churn_clientes.sql
│   └── analise_sazonalidade.sql
│
├── .github/workflows/
│   └── deploy-docs.yml             # CI/CD para dbt docs
│
├── dbt_project.yml
├── profiles.yml.example
└── README.md
```

---

## 🔧 Stack Tecnológica

| Componente | Tecnologia | Motivo da Escolha |
|---|---|---|
| Transformação | dbt Core 1.7 | Padrão de mercado, versionamento SQL |
| Data Warehouse | BigQuery | Custo por query, escala serverless |
| Orquestração | Cloud Composer (Airflow) | Gerenciado, integração GCP nativa |
| Ingestão | Airbyte (self-hosted) | Open source, 300+ conectores |
| BI | Looker Studio + Metabase | Self-service + dashboards executivos |
| CI/CD | GitHub Actions | Deploy automático de docs |
| Qualidade | dbt tests + Great Expectations | Dupla camada de validação |

---

## 🚀 Como Executar

### Pré-requisitos
```bash
python >= 3.9
dbt-bigquery >= 1.7.0
```

### Instalação
```bash
# Clone o repositório
git clone https://github.com/carlosrodrigodados/dbt-varejo-analytics.git
cd dbt-varejo-analytics

# Instale as dependências
pip install dbt-bigquery

# Configure o profiles.yml
cp profiles.yml.example ~/.dbt/profiles.yml
# Edite com suas credenciais do BigQuery
```

### Execução
```bash
# Instalar dependências dbt
dbt deps

# Carregar seeds (dados de referência)
dbt seed

# Executar todos os modelos
dbt run

# Rodar testes de qualidade
dbt test

# Gerar documentação
dbt docs generate
dbt docs serve  # Acessa em http://localhost:8080
```

### Executar por camada
```bash
# Apenas staging
dbt run --select staging

# Apenas um domínio
dbt run --select marts.comercial

# Modelo específico e seus dependentes
dbt run --select +fct_vendas
```

---

## ✅ Cobertura de Testes

| Camada | Modelos | Testes Genéricos | Testes Customizados | Cobertura |
|---|---|---|---|---|
| Staging | 8 | 34 | 0 | 100% |
| Intermediate | 4 | 18 | 6 | 100% |
| Marts | 9 | 41 | 8 | 94% |
| **Total** | **21** | **93** | **14** | **94%** |

---

## 📚 Documentação ao Vivo

A documentação completa com lineage, descrições e testes está publicada em:

👉 **[https://carlosrodrigodados.github.io/dbt-varejo-analytics](https://carlosrodrigodados.github.io/dbt-varejo-analytics)**

---

## 🧠 Decisões de Arquitetura

### Por que unificar PDV e E-commerce em `int_vendas_unificadas`?
O principal problema de negócio era a divergência entre canais. Criamos uma camada intermediate que aplica as mesmas regras de negócio para ambos os canais antes de chegar nos marts, garantindo que qualquer relatório use a mesma definição de "venda concluída".

### Por que BigQuery ao invés de Databricks SQL?
O volume de dados (50GB/dia) não justificava o custo de um cluster Databricks ativo. BigQuery com particionamento por data e clustering por loja_id reduziu o custo de R$ 28.000 para R$ 9.400/mês. Para volumes acima de 500GB/dia, Databricks seria a escolha.

### Por que três camadas e não duas?
A camada intermediate foi essencial para encapsular regras de negócio complexas (ex: cálculo de RFM, unificação de canais) sem duplicá-las em múltiplos marts. Qualquer mudança de regra é feita em um único lugar.

---

## 👤 Autor

**Carlos Rodrigo Bezerra de Sousa**
Analytics Engineer | Data & AI Specialist

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Carlos_Rodrigo-blue)](https://linkedin.com/in/carlosrodrigodados)
[![Certificações](https://img.shields.io/badge/IBM-AI_Engineering-orange)](https://coursera.org)
[![Databricks](https://img.shields.io/badge/Databricks-Certified-red)](https://databricks.com)
