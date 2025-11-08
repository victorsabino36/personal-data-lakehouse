📊 Personal Data Lakehouse: Pipeline ELT de Ações FinanceirasRepositório contendo o projeto de Engenharia de Dados que implementa um Data Lakehouse completo na Google Cloud Platform (GCP). O objetivo é demonstrar proficiência em arquitetura ELT (Extract, Load, Transform), modelagem dbt e automação de pipelines usando GitHub Actions (Infraestrutura como Código - IaC).💡 Objetivo do ProjetoO objetivo principal é simular um pipeline de dados financeiros para consumo, processando dados diários de ações de mercado (tickers: IBM, MSFT, NVDA). O foco é na qualidade do código, incrementalidade e automação robusta, demonstrando o ciclo de vida completo do dado.🏗️ Arquitetura e Estrutura do RepositórioO projeto segue a arquitetura Lakehouse, separando as responsabilidades de ingestão, transformação e orquestração.personal-data-lakehouse/
├── .github/                   # AUTOMAÇÃO & DEVOPS
│   └── workflows/
│       └── data_pipeline.yml  # Pipeline ELT agendado via GitHub Actions
│
├── dbt/                       # TRANSFORMAÇÃO (T) & MODELAGEM
│   ├── lakehouse_models/
│   │   ├── models/
│   │   │   ├── bronze/        # Definição da Fonte Externa (Source)
│   │   │   ├── silver/        # Dados limpos e padronizados
│   │   │   └── gold/          # Agregações para consumo (Data Mart)
│   └── requirements.txt
│
├── pipelines/                 # EXTRAÇÃO E CARGA (E & L)
│   └── ingest_stock_api/
│       └── ingest_stocks.py   # Script de ingestão da API Alpha Vantage
├── .gitignore
└── README.md
🛠️ Tecnologias UtilizadasCategoriaFerramentaPropósito no PipelineInfraestrutura CloudGoogle Cloud Platform (GCP)Hospedagem principal.Data WarehouseBigQueryPlataforma de processamento SQL e destino final dos dados.Modelagem/ELTdbt (Data Build Tool)Materialização incremental, testes de qualidade e modelagem Silver/Gold.Orquestração (IaC)GitHub ActionsAgendamento diário e execução automatizada do workflow Python → dbt.Ingestão/ExtraçãoPython (requests, pandas)Busca de dados da API e carregamento inicial em Parquet/GCS.Formato de DadosParquetArmazenamento colunar eficiente no Data Lake (GCS).💧 Camadas do Data Lakehouse (BigQuery)O pipeline garante a qualidade e a performance segregando os dados em camadas:CamadaDataset de DestinoPropósitoMaterializaçãoBronzestock_bronzeDados brutos, sem tratamento, mas formatados (Parquet no GCS).TRUNCATE (Atualizado diariamente)Silverstock_silverDados limpos, com tipos e timestamps corrigidos. Base para transformações.IncrementalGoldstock_goldAgregações de negócio (Ex: Resumo Mensal). Otimizado para BI e Análise.Incremental🚀 Configuração e AutomaçãoPré-requisitosPython 3.11+dbt-bigquery (pip install dbt-bigquery)Conta GCP.1. Autenticação na NuvemO pipeline usa o método seguro Workload Identity Federation para autenticar o GitHub Actions no GCP.Crie uma Service Account (ex: github-actions-sa) com papéis BigQuery Data Editor, Storage Admin, e Service Account Token Creator.Configure os Repository Secrets no GitHub:ALPHA_VANTAGE_API_KEYGCP_PROJECT_NUMBER (O número do seu projeto GCP).2. Execução Local (Para Desenvolvimento)Para testar o pipeline completo localmente (simulando o Actions):# 1. Instala dependências
pip install -r ./pipelines/requirements.txt -r ./dbt/requirements.txt

# 2. Testa a ingestão (Extrai API -> Carrega Bronze)
python ./pipelines/ingest_stock_api/ingest_stocks.py

# 3. Testa a transformação (Bronze -> Silver -> Gold)
cd dbt/lakehouse_models
dbt run --full-refresh # Roda tudo do zero para validação
dbt test # Executa os testes de qualidade
🤝 ContribuiçãoSiga o fluxo padrão de desenvolvimento:Crie um branch para o recurso: git checkout -b feature/nova-featureFaça o commit das alterações e push.Abra um Pull Request para o branch main.