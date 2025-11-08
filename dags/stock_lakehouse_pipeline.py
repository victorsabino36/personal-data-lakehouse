from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago

# --- CONFIGURAÇÃO DE CAMINHOS ---
# Usamos caminhos relativos ao diretório base onde o Airflow encontra o DAG (geralmente /gcs/dags/)
# Se o repositório for clonado para a pasta 'dags' do GCS, o caminho será este:
BASE_DIR = "/home/airflow/gcs/dags/personal-data-lakehouse" # <-- Ajuste para o nome da sua pasta de projeto no GCS
DBT_DIR = f"{BASE_DIR}/dbt/lakehouse_models"
PYTHON_INGEST_PATH = f"{BASE_DIR}/pipelines/ingest_stock_api/main.py" 
# ---------------------------------

# Funções auxiliares para o branching (decisão)
def check_execution_mode(**kwargs):
    """Verifica o parâmetro 'mode' (daily ou rebuild) passado ao rodar a DAG."""
    # Retorna o ID da próxima tarefa a ser executada.
    execution_mode = kwargs['dag_run'].conf.get('mode', 'daily') 
    
    if execution_mode == 'rebuild':
        return 'run_silver_full_refresh'
    else:
        return 'run_silver_incremental'

with DAG(
    dag_id='stock_lakehouse_pipeline',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
    },
    tags=['data_lakehouse', 'dbt', 'gcp', 'stock'],
    # Permite ao usuário escolher o modo via UI do Airflow
    params={"mode": {"type": "string", "default": "daily", "enum": ["daily", "rebuild"]}},
) as dag:

    # 1. TAREFA: Ingestão Python (Camada Bronze - Sempre TRUNCATE)
    ingest_bronze = BashOperator(
        task_id='ingest_bronze_data',
        # Executa o script Python
        bash_command=f"python {PYTHON_INGEST_PATH}", 
    )

    # 2. TAREFA: Branching (Decisão de Lógica)
    branch_mode_check = BranchPythonOperator(
        task_id='check_execution_mode',
        python_callable=check_execution_mode,
        provide_context=True,
    )

    # 3. TAREFA: Transformação Silver - Incremental
    run_silver_incremental = BashOperator(
        task_id='run_silver_incremental',
        # Roda o dbt sem flags, usando a lógica incremental do modelo SQL
        bash_command=f"cd {DBT_DIR} && dbt run --target dev --select daily_stocks",
        env={'DBT_PROFILES_DIR': '~/.dbt'}, 
    )

    # 4. TAREFA: Transformação Silver - Full Reprocess
    run_silver_full_refresh = BashOperator(
        task_id='run_silver_full_refresh',
        # Roda o dbt com --full-refresh para recriar a tabela Silver do zero
        bash_command=f"cd {DBT_DIR} && dbt run --target dev --select daily_stocks --full-refresh",
        env={'DBT_PROFILES_DIR': '~/.dbt'}, 
    )
    
    # 5. TAREFA (Opcional, mas profissional): Testes de Qualidade de Dados
    dbt_test = BashOperator(
        task_id='dbt_test_silver',
        bash_command=f"cd {DBT_DIR} && dbt test",
        env={'DBT_PROFILES_DIR': '~/.dbt'}, 
    )

    # --- DEFINIÇÃO DO FLUXO ---
    ingest_bronze >> branch_mode_check
    
    # O Branch envia o fluxo para um dos dois caminhos Silver
    branch_mode_check >> run_silver_incremental >> dbt_test
    branch_mode_check >> run_silver_full_refresh >> dbt_test