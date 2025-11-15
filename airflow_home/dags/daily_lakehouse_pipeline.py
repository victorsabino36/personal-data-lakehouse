import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# !! CORREÇÃO 1: Mudar o nome do Operador !!
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitBatchOperator

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
# O GCS connector já é incluído por padrão no Dataproc Serverless
PYSPARK_PACKAGES = ["io.delta:delta-spark_2.12:3.2.0"]

# Caminho local (NO WORKER DO COMPOSER) para o projeto dbt
DBT_PROJECT_LOCAL_PATH = "/home/airflow/gcs/dags/dbt/lakehouse_models"

# ============================================================================
# COMANDOS BASH (Apenas dbt)
# ============================================================================

CMD_RUN_DBT_MODELS = f"""
echo "-----------------------------------"
echo "Iniciando Task [dbt run (Silver + Gold)]"
echo "Pasta do Projeto dbt: {DBT_PROJECT_LOCAL_PATH}"
echo "-----------------------------------"
cd {DBT_PROJECT_LOCAL_PATH} && \
echo "Executando dbt run..."

# Puxa a chave JSON da Variável do Airflow e a exporta
RAW_KEY_JSON='{{{{ var.value.GCP_SA_KEYFILE_JSON }}}}'
export DBT_GCP_KEYFILE=$(echo "$RAW_KEY_JSON" | tr -d '\n')

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
    dag_id='daily_lakehouse_pipeline_cloud', 
    default_args=default_args,
    description='Pipeline Cloud-Native: Dataproc (PySpark) e Composer (dbt).',
    schedule_interval='@daily',
    catchup=False,
    tags=['lakehouse', 'daily', 'gcp', 'dataproc', 'dbt'],
) as dag:

    # Task 1: Ingestão de Criptomoedas (Dataproc)
    # !! CORREÇÃO 2: Usar DataprocSubmitBatchOperator e a estrutura 'batch' !!
    task_ingest_crypto = DataprocSubmitBatchOperator(
        task_id="ingest_crypto_bronze_dataproc",
        project_id=PROJECT_ID,
        region=GCP_REGION,
        gcp_conn_id="google_cloud_default",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": CRYPTO_SCRIPT_GCS_PATH,
            },
            "runtime_config": {
                "properties": {
                    # Os pacotes são passados como 'properties'
                    "spark.jars.packages": ",".join(PYSPARK_PACKAGES)
                }
            }
        }
    )

    # Task 2: Ingestão de Ações (Dataproc)
    # !! CORREÇÃO 2: Usar DataprocSubmitBatchOperator e a estrutura 'batch' !!
    task_ingest_stocks = DataprocSubmitBatchOperator(
        task_id="ingest_stocks_bronze_dataproc",
        project_id=PROJECT_ID,
        region=GCP_REGION,
        gcp_conn_id="google_cloud_default",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": STOCKS_SCRIPT_GCS_PATH,
                # Os argumentos são passados dentro do 'pyspark_batch'
                "args": ["{{ var.value.ALPHA_VANTAGE_API_KEY }}"]
            },
            "runtime_config": {
                "properties": {
                    "spark.jars.packages": ",".join(PYSPARK_PACKAGES)
                }
            }
        }
    )

    # Task 3: Rodar todos os modelos dbt (Bash)
    task_run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command=CMD_RUN_DBT_MODELS,
    )

    # --- 5. Definindo as Dependências ---
    task_ingest_stocks >> task_run_dbt
    # task_ingest_crypto (roda em paralelo)