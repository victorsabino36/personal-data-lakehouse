import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# --- 1. Definição de Caminhos ---
DAG_FILE_PATH = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(DAG_FILE_PATH)))


# --- 2. Caminhos para os Scripts e Projetos ---
CRYPTO_SCRIPT_PATH = os.path.join(PROJECT_ROOT, 'pipelines', 'process_crypto_pyspark')
STOCKS_SCRIPT_PATH = os.path.join(PROJECT_ROOT, 'pipelines', 'ingest_stock_api') 
DBT_PROJECT_PATH = os.path.join(PROJECT_ROOT, 'dbt', 'lakehouse_models') # Caminho correto do projeto dbt


# --- Caminhos dos VENVs ---
PYSPARK_VENV_ACTIVATE = os.path.join(PROJECT_ROOT, 'pipelines', 'process_crypto_pyspark', 'venv', 'bin', 'activate')
STOCK_VENV_ACTIVATE = os.path.join(PROJECT_ROOT, 'pipelines', 'ingest_stock_api', 'venv', 'bin', 'activate')
DBT_VENV_ACTIVATE = os.path.join(PROJECT_ROOT, 'dbt', 'venv', 'bin', 'activate')


# --- 3. Comandos Bash para cada Tarefa ---

# Task 1: Ingestão de Cripto (PySpark)
CMD_INGEST_CRYPTO = f"""
echo "-----------------------------------"
echo "Iniciando Task [Ingestão Cripto Bronze]"
echo "Ativando VENV em: {PYSPARK_VENV_ACTIVATE}"
echo "-----------------------------------"
cd {CRYPTO_SCRIPT_PATH} && \
source {PYSPARK_VENV_ACTIVATE} && \
python ingest_crypto_bronze.py
"""

# Task 2: Ingestão de Ações (PySpark)
CMD_INGEST_STOCKS = f"""
echo "-----------------------------------"
echo "Iniciando Task [Ingestão Ações Bronze]"
export ALPHA_VANTAGE_API_KEY="{{{{ var.value.ALPHA_VANTAGE_API_KEY }}}}"
echo "Ativando VENV em: {STOCK_VENV_ACTIVATE}"
echo "-----------------------------------"
cd {STOCKS_SCRIPT_PATH} && \
source {STOCK_VENV_ACTIVATE} && \
python ingest_stocks.py 
"""


# RODA O DBT
CMD_RUN_DBT_MODELS = f"""
echo "-----------------------------------"
echo "Rodando dbt com OAuth (ADC)"
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/gcloud/application_default_credentials.json"
cd {DBT_PROJECT_PATH} && \
source {DBT_VENV_ACTIVATE} && \
dbt run --profiles-dir .
"""



# --- 4. Definição da DAG ---
default_args = {
    'owner': 'VictorSabino',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='daily_lakehouse_pipeline',
    default_args=default_args,
    description='Pipeline diária: Ingestão (PySpark) e Transformação (dbt).',
    schedule='@daily',
    catchup=False,
    tags=['lakehouse', 'daily', 'stocks', 'crypto', 'dbt'],
) as dag:

    # Task 1: Ingestão de Criptomoedas (PySpark)
    task_ingest_crypto = BashOperator(
        task_id='ingest_crypto_bronze',
        bash_command=CMD_INGEST_CRYPTO,
    )

    # Task 2: Ingestão de Ações (PySpark)
    task_ingest_stocks = BashOperator(
        task_id='ingest_stocks_bronze',
        bash_command=CMD_INGEST_STOCKS,
    )

    # Task 3: Rodar todos os modelos dbt
    task_run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command=CMD_RUN_DBT_MODELS,
    )

    # --- 5. Definindo as Dependências ---
    task_ingest_stocks >> task_run_dbt
    # task_ingest_crypto (roda em paralelo)