import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Operador CORRETO para Dataproc Serverless (Batch)
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# ============================================================================
# CONSTANTES (Cloud-Native)
# ============================================================================
PROJECT_ID = "personal-data-lakehouse"
GCP_REGION = "us-central1"

# Caminhos no GCS (deploy feitos pelo GitHub Actions)
ARTIFACTS_BUCKET = "personal-data-lakehouse-artifacts"
CRYPTO_SCRIPT_GCS_PATH = f"gs://{ARTIFACTS_BUCKET}/pipelines/process_crypto_pyspark/ingest_crypto_bronze.py"
STOCKS_SCRIPT_GCS_PATH = f"gs://{ARTIFACTS_BUCKET}/pipelines/ingest_stock_api/ingest_stocks.py"

# Pacotes PySpark necessários no Dataproc Serverless
PYSPARK_PACKAGES = ["io.delta:delta-spark_2.13:3.2.0"] 

# Caminho do projeto dbt (mount automático do Composer)
DBT_PROJECT_LOCAL_PATH = "/home/airflow/gcs/dags/dbt/lakehouse_models"

# ============================================================================
# COMANDOS BASH (dbt)
# ============================================================================
CMD_RUN_DBT_MODELS = f"""
echo "-----------------------------------"
echo "Iniciando Task [dbt run - Silver + Gold]"
echo "Pasta do Projeto dbt: {DBT_PROJECT_LOCAL_PATH}"
echo "-----------------------------------"
cd {DBT_PROJECT_LOCAL_PATH}
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
    description='Pipeline Cloud-Native: Dataproc (PySpark Serverless) + Composer (dbt).',
    schedule_interval='@daily',
    catchup=False,
    tags=['lakehouse', 'daily', 'gcp', 'dataproc', 'dbt'],
) as dag:

    # ----------------------------------------------------------------------
    # Task 1 — Ingestão de Criptomoedas (PySpark -> Bronze)
    # ----------------------------------------------------------------------
    task_ingest_crypto = DataprocCreateBatchOperator(
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
                "spark.jars.packages": ",".join(PYSPARK_PACKAGES),
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            }
        }
    }
        )

    task_ingest_stocks = DataprocCreateBatchOperator(
        task_id="ingest_stocks_bronze_dataproc",
        project_id=PROJECT_ID,
        region=GCP_REGION,
        gcp_conn_id="google_cloud_default",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": STOCKS_SCRIPT_GCS_PATH,
                "args": ["{{ var.value.ALPHA_VANTAGE_API_KEY }}"],
            },
            "runtime_config": {
                "properties": {
                    "spark.jars.packages": ",".join(PYSPARK_PACKAGES),
                    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                }
            }
        }
    )


    # ----------------------------------------------------------------------
    # Task 3 — Rodar modelos dbt (Silver + Gold)
    # ----------------------------------------------------------------------
    task_run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command=CMD_RUN_DBT_MODELS,
    )

    # ----------------------------------------------------------------------
    # Dependências
    # Crypto roda em paralelo, Stocks precisa terminar → dbt roda depois
    # ----------------------------------------------------------------------
    task_ingest_stocks >> task_run_dbt
