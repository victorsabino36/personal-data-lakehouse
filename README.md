# 📊 Personal Data Lakehouse: 

Repositório contendo o projeto de **Engenharia de Dados** que implementa um *Data Lakehouse* completo na **Google Cloud Platform (GCP)**. O objetivo é demonstrar proficiência em arquitetura **ETL** (Extract,  Transform, Load), modelagem **dbt** e automação de *pipelines* usando **GitHub Actions** (Infraestrutura como Código - IaC).

---

## 💡 Objetivo do Projeto

O objetivo principal é simular um *pipeline* de dados financeiros para consumo, processando dados diários de ações de mercado (tickers: IBM, MSFT, NVDA). O foco é na **qualidade do código**, **incrementalidade** e **automação robusta**, demonstrando o ciclo de vida completo do dado.

## 🏗️ Arquitetura e Estrutura do Repositório

O projeto segue a arquitetura **Lakehouse**, separando as responsabilidades de ingestão, transformação e orquestração.

```tree
personal-data-lakehouse/
├── .github/                   # CONFIGURAÇÃO DE CI/CD
│   └── workflows/             # (Contém data_pipeline.yml)
├── dags/                      # ORQUESTRAÇÃO (Estrutura para Airflow/Composer)
├── dbt/                       # TRANSFORMAÇÃO & MODELAGEM (ELT - T)
│   └── lakehouse_models/
│       ├── models/
│       │   ├── bronze/        # Definição das fontes externas (sources)
│       │   ├── silver/        # Modelos limpos e padronizados
│       │   └── gold/          # Modelos agregados para BI (Data Marts)
│       └── requirements.txt
├── pipelines/                 # CÓDIGO DE INGESTÃO (ETL)
│   
├── .gitignore
└── README.md                  # Documentação principal
```

## 🛠️ Tecnologias Utilizadas

| Categoria | Ferramenta | Propósito no Pipeline |
| :--- | :--- | :--- |
| **Infraestrutura Cloud** | **Google Cloud Platform (GCP)** | Hospedagem principal. |
| **Data Warehouse** | **BigQuery** | Plataforma de processamento SQL e destino final dos dados. |
| **Modelagem/ELT** | **dbt (Data Build Tool)** | Materialização incremental, testes de qualidade e modelagem Silver/Gold. |
| **Orquestração (IaC)** | **GitHub Actions** | Agendamento diário e execução automatizada do *workflow* Python → dbt. |
| **Ingestão/Extração**| **Python** (`requests`, `pandas`) | Busca de dados da API e carregamento inicial em Parquet/GCS. |
| **Formato de Dados** | **Parquet** | Armazenamento colunar eficiente no Data Lake (GCS). |

---

## 💧 Camadas do Data Lakehouse (BigQuery)

O pipeline garante a qualidade e a performance segregando os dados em camadas:

| Camada | Dataset de Destino | Propósito | Materialização |
| :--- | :--- | :--- | :--- |
| **Bronze** | `{}_bronze` | Dados brutos, sem tratamento, mas formatados (Parquet no GCS). 
| **Silver** | `{}_silver` | Dados limpos, com tipos e *timestamps* corrigidos. Base para transformações. 
| **Gold** | `{}_gold` | Agregações de negócio (Ex: Resumo Mensal). Otimizado para BI e Análise. 

---

## 🚀 Configuração e Automação

### Pré-requisitos

* Python 3.11+
* dbt-bigquery (`pip install dbt-bigquery`)
* Conta GCP.

### 1. Autenticação na Nuvem

O pipeline usa o método seguro **Workload Identity Federation** para autenticar o GitHub Actions no GCP.

1.  Crie uma Service Account (ex: `github-actions-sa`) com papéis `BigQuery Data Editor`, `Storage Admin`, e `Service Account Token Creator`.
2.  Configure os seguintes **Repository Secrets** no GitHub:
    * `ALPHA_VANTAGE_API_KEY`
    * `GCP_PROJECT_NUMBER`

### 2. Execução Local (Para Desenvolvimento)

Para testar o pipeline completo localmente (simulando o Actions):

```bash
# 1. Instala dependências
pip install -r ./pipelines/requirements.txt -r ./dbt/requirements.txt

# 2. Testa a ingestão (Extrai API -> Carrega Bronze)
python ./pipelines/ingest_stock_api/ingest_stocks.py

# 3. Testa a transformação (Bronze -> Silver -> Gold)
cd dbt/lakehouse_models
dbt run --full-refresh # Roda tudo do zero para validação
dbt test # Executa os testes de qualidade


