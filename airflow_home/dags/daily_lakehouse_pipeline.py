import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkBatchOperator
# ============================================================================
# CONSTANTES (Cloud-Native)
# ============================================================================
PROJECT_ID = "personal-data-lakehouse"
GCP_REGION = "us-central1" # Região do seu Composer/Dataproc

# Caminhos no GCS (Onde o GHA faz o deploy)
ARTIFACTS_BUCKET = "personal-data-lakehouse-artifacts"
CRYPTO_SCRIPT_GCS_PATH = f"gs://{ARTIFACTS_BUCKET}/pipelines/process_crypto_pyspark/ingest_crypto_bronze.py"
STOCKS_SCRIPT_GCS_PATH = f"gs://{ARTIFACTS_BUCKET}/pipelines/ingest_stock_api/ingest_stocks.py"

# Pacotes que o Dataproc Serverless precisa baixar
PYSPARK_PACKAGES = ["io.delta:delta-spark_2.12:3.2.0"]

# Caminho local (NO WORKER DO COMPOSER) para o projeto dbt
# O GHA copiou o projeto para 'gs://.../dags/dbt'
# O Composer monta esse bucket em '/home/airflow/gcs/dags/'
DBT_PROJECT_LOCAL_PATH = "/home/airflow/gcs/dags/dbt/lakehouse_models"

# ============================================================================
# COMANDOS BASH (Apenas dbt)
# ============================================================================

# Comando para rodar o dbt
CMD_RUN_DBT_MODELS = f"""
echo "-----------------------------------"
echo "Iniciando Task [dbt run (Silver + Gold)]"
echo "Pasta do Projeto dbt: {DBT_PROJECT_LOCAL_PATH}"
echo "-----------------------------------"

# 1. 'cd' para a pasta do projeto dbt
cd {DBT_PROJECT_LOCAL_PATH} && \

echo "Executando dbt run..."
# 2. Executa dbt run
# (O profiles.yml está na pasta do projeto e usa 'service_account_json')
# (A Key 'DBT_GCP_KEYFILE' será lida das Variáveis do Airflow)
dbt run --profiles-dir .
"""

# ============================================================================
# DEFINIÇÃO DA DAG
# ============================================================================
default_args = {
    'owner': 'VictorSabino',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_lakehouse_pipeline_cloud', # Novo nome
    default_args=default_args,
    description='Pipeline Cloud-Native: Dataproc (PySpark) e Composer (dbt).',
    schedule_interval='@daily',
    catchup=False,
    tags=['lakehouse', 'daily', 'gcp', 'dataproc', 'dbt'],
) as dag:

    # Task 1: Ingestão de Criptomoedas (Dataproc)
    task_ingest_crypto = DataprocSubmitPySparkJobOperator(
        task_id='ingest_crypto_bronze_dataproc',
        main=CRYPTO_SCRIPT_GCS_PATH,
        project_id=PROJECT_ID,
        region=GCP_REGION,
        gcp_conn_id="google_cloud_default", 
        dataproc_jars_in_spark_conf=True, 
        dataproc_spark_packages=PYSPARK_PACKAGES 
    )

    # Task 2: Ingestão de Ações (Dataproc)
    task_ingest_stocks = DataprocSubmitPySparkJobOperator(
        task_id='ingest_stocks_bronze_dataproc',
        main=STOCKS_SCRIPT_GCS_PATH,
        project_id=PROJECT_ID,
        region=GCP_REGION,
        gcp_conn_id="google_cloud_default",
        dataproc_jars_in_spark_conf=True,
        dataproc_spark_packages=PYSPARK_PACKAGES,
        # Passa a API Key (lida das Variáveis do Airflow) como argumento
        # para o script (sys.argv[1])
        pyfiles_uris=[],
        args=["{{ var.value.ALPHA_VANTAGE_API_KEY }}"] 
    )

    # Task 3: Rodar todos os modelos dbt (Bash)
    task_run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command=CMD_RUN_DBT_MODELS,
    )

    # --- 5. Definindo as Dependências ---
    task_ingest_stocks >> task_run_dbt
    # task_ingest_crypto (roda em paralelo)