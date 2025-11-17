import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunCreateJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators.bash import BashOperator

PROJECT_ID = "personal-data-lakehouse"
GCP_REGION = "us-central1"

ARTIFACTS_BUCKET = "personal-data-lakehouse-artifacts"
CRYPTO_SCRIPT_GCS_PATH = f"gs://{ARTIFACTS_BUCKET}/pipelines/process_crypto_pyspark/ingest_crypto_bronze.py"
STOCKS_SCRIPT_GCS_PATH = f"gs://{ARTIFACTS_BUCKET}/pipelines/ingest_stock_api/ingest_stocks.py"

PYSPARK_PACKAGES = ["io.delta:delta-spark_2.13:3.2.0"] 

DBT_PROJECT_MAPPED_PATH = "/opt/airflow/dags/dbt"
DBT_JOB_NAME = "dbt-lakehouse-transformation"

DATAPROC_SERVICE_ACCOUNT = "github-actions-sa@personal-data-lakehouse.iam.gserviceaccount.com"

default_args = {
    'owner': 'VictorSabino',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_lakehouse_pipeline',
    default_args=default_args,
    description='Pipeline Cloud-Native: Dataproc + dbt',
    schedule_interval='@daily',
    catchup=False,
    tags=['lakehouse', 'daily', 'gcp', 'dataproc', 'dbt'],
) as dag:

        # TASK 1 - Crypto
    task_ingest_crypto = DataprocCreateBatchOperator(
        task_id="ingest_crypto_bronze_dataproc",
        project_id=PROJECT_ID,
        region=GCP_REGION,
        gcp_conn_id="google_cloud_default",
        batch_id="ingest-crypto-{{ ds_nodash }}",
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
            },
            "environment_config": {
                "execution_config": {
                    "service_account": DATAPROC_SERVICE_ACCOUNT
                }
            }
        }
    )


    # TASK 2 - Stocks
    task_ingest_stocks = DataprocCreateBatchOperator(
        task_id="ingest_stocks_bronze_dataproc",
        project_id=PROJECT_ID,
        region=GCP_REGION,
        gcp_conn_id="google_cloud_default",
        batch_id="ingest-stocks-{{ ds_nodash }}",
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
            },
            "environment_config": {
                "execution_config": {
                    "service_account": DATAPROC_SERVICE_ACCOUNT
                }
            }
        }
    )


    # TASK 3 - dbt tests


    # TASK 4 - dbt transformation in Cloud Run
    task_run_dbt = CloudRunCreateJobOperator(
        task_id="run_dbt_models_cloud_run",
        project_id=PROJECT_ID,
        region=GCP_REGION,
        gcp_conn_id="google_cloud_default",
        job_name=DBT_JOB_NAME,
        job={
            "template": {
                "template": {
                    "containers": [{
                        "image": f"gcr.io/{PROJECT_ID}/dbt_runner:latest",
                        "args": ["run", "--profiles-dir", "/usr/app"], 
                    }],
                    "serviceAccount": DATAPROC_SERVICE_ACCOUNT
                }
            }
        }
    )

    [task_ingest_crypto, task_ingest_stocks] >> task_run_dbt