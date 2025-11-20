import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime
import unicodedata
import re
import os

# --- CONFIGURAÇÕES ---
DATASET_ID = 'desafio_droove' 
LOCATION = 'US' 

files_to_process = {
    'bitcoin':        'data/Desafio - Bitcoin.csv',
    'bova11':         'data/Desafio - Bova11.csv',
    'dolar':          'data/Desafio - Dolar.csv',
    'ethereum':       'data/Desafio - Ethereum.csv',
    'indice_di':      'data/Desafio - Indice DI.csv',
    'indice_ipca':    'data/Desafio - Indice IPCA.csv',
    'interest_index': 'data/Desafio - Interest Index.csv',
    'smal11':         'data/Desafio - Smal11.csv',
    'taxa_selic':     'data/Desafio - Taxa Selic.csv'
}

def create_dataset_if_not_exists(client, dataset_id):
    """Garante a existência do dataset no BigQuery."""
    dataset_ref = f"{client.project}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' verificado: Já existe.")
    except NotFound:
        print(f"Dataset '{dataset_id}' não encontrado. Criando...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = LOCATION
        client.create_dataset(dataset, timeout=30)
        print(f"Dataset '{dataset_id}' criado com sucesso.")

def sanitize_column_names(df):
    """
    Padroniza nomes de colunas e garante que nenhuma fique vazia.
    """
    new_columns = []
    for i, col in enumerate(df.columns):
        # Se a coluna for do tipo 'Unnamed' (erro comum do Pandas), renomeia
        if str(col).startswith('Unnamed'):
            new_columns.append(f"coluna_extra_{i}")
            continue

        # Normaliza (remove acentos)
        col_str = unicodedata.normalize('NFKD', str(col)).encode('ASCII', 'ignore').decode('utf-8')
        col_str = col_str.lower().replace(' ', '_')
        
        # Remove caracteres especiais
        clean_col = re.sub(r'[^a-z0-9_]', '', col_str)
        
        # --- CORREÇÃO DO ERRO ---
        # Se após a limpeza o nome ficar vazio (ex: coluna era só "%"), gera um nome
        if not clean_col:
            clean_col = f"coluna_sem_nome_{i}"
            
        # Se o nome começar com número (proibido no BQ), adiciona prefixo
        if clean_col[0].isdigit():
            clean_col = f"num_{clean_col}"

        new_columns.append(clean_col)
    
    df.columns = new_columns
    return df

def load_dataframe_to_bq(client, dataframe, table_name, dataset_id):
    """Carrega o DataFrame para o BigQuery."""
    table_id = f"{client.project}.{dataset_id}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    print(f"   -> Uploading: {table_name}...")
    try:
        job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
        job.result()
        print(f"   ✅ Tabela {table_name} carregada ({job.output_rows} linhas).")
    except Exception as e:
        print(f"   ❌ Falha no upload de {table_name}: {e}")

# --- EXECUÇÃO ---
if __name__ == "__main__":
    print("--- Iniciando Pipeline ETL ---")
    
    client = bigquery.Client()
    create_dataset_if_not_exists(client, DATASET_ID)
    
    print("-" * 40)

    for table_name, file_path in files_to_process.items():
        print(f"\nProcessando: {table_name.upper()}")
        
        try:
            # 1. Leitura
            df = pd.read_csv(file_path, sep=',', encoding='utf-8')
            
            # 2. Transformação (Limpeza Robustecida + Timestamp)
            df = sanitize_column_names(df)
            df['received_at'] = datetime.now()
            
            # 3. Carga
            load_dataframe_to_bq(client, df, table_name, DATASET_ID)
            
        except FileNotFoundError:
            print(f"   ⚠️ Arquivo não encontrado: {file_path}")
        except Exception as e:
            print(f"   ⚠️ Erro crítico: {e}")

    print("\n--- Pipeline Finalizado ---")