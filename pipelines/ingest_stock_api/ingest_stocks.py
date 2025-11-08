import os
import requests
from google.cloud import bigquery, storage
from dotenv import load_dotenv
import time
import pandas as pd # <-- 1. Importa o pandas

# --- Configuração ---
load_dotenv() 

PROJECT_ID = "personal-data-lakehouse" 
DATASET_ID = "personal_lake_stock_bronze" 
TABLE_ID = "raw_stock_daily"
STOCK_TICKERS = ["IBM", "MSFT", "NVDA"]

#  Nome do Bucket e Formato do Arquivo 
GCS_BUCKET_NAME = "date_lakehouse_bronze" # <-- Nome do seu bucket
GCS_BLOB_NAME = f"raw_stock_data_{time.strftime('%Y%m%d')}.parquet" # <-- MUDAMOS para .parquet


ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

if not ALPHA_VANTAGE_API_KEY:
    raise ValueError("Chave ALPHA_VANTAGE_API_KEY não encontrada. Verifique seu arquivo .env")

def ingest_stock_data():
    """
    Passo 1: Busca dados da API da Alpha Vantage.
    Passo 2: Salva os dados brutos como PARQUET no Google Cloud Storage (Data Lake).
    Passo 3: Carrega os dados PARQUET do GCS para o BigQuery (Data Warehouse).
    """
    print(f"Iniciando ingestão para os tickers: {STOCK_TICKERS}")
    
    todos_os_dados = []
    for ticker in STOCK_TICKERS:
        print(f"\nBuscando dados para: {ticker}")
        url = (
            f"https://www.alphavantage.co/query?"
            f"function=TIME_SERIES_DAILY" 
            f"&symbol={ticker}"
            f"&outputsize=compact"
            f"&apikey={ALPHA_VANTAGE_API_KEY}"
        )
        
        try:
            response = requests.get(url)
            response.raise_for_status() 
            data = response.json()
            time_series_data = data.get('Time Series (Daily)')
            
            if not time_series_data:
                print(f"Nenhum dado encontrado para {ticker}. Resposta da API: {data}")
                time.sleep(15) 
                continue 

            for data_str, valores in time_series_data.items():
                registro = {
                    "ticker": ticker, 
                    "date": data_str,
                    "open": float(valores.get("1. open")),
                    "high": float(valores.get("2. high")),
                    "low": float(valores.get("3. low")),
                    "close": float(valores.get("4. close")),
                    "volume": int(valores.get("5. volume")),
                }
                todos_os_dados.append(registro)

            print(f"API retornou {len(time_series_data)} registros diários para {ticker}.")
            print("Pausando 15s para evitar o rate limit da API...")
            time.sleep(15) 

        except Exception as e:
            print(f"Um erro inesperado ocorreu na ingestão de {ticker}: {e}")
            continue

    if not todos_os_dados:
        print("Nenhum dado foi coletado. Encerrando.")
        return

    # **** MUDANÇA 2: Converter para Pandas e Salvar como PARQUET no GCS ****
    try:
        print(f"\nConvertendo {len(todos_os_dados)} registros para o DataFrame do Pandas...")
        df = pd.DataFrame(todos_os_dados)
        
        # Converte as colunas para os tipos corretos (boa prática)
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        
        gcs_uri = f"gs://{GCS_BUCKET_NAME}/{GCS_BLOB_NAME}"
        print(f"Salvando dados brutos como Parquet no GCS: {gcs_uri}")
        
        # O Pandas (com gcsfs) escreve DIRETAMENTE no GCS.
        df.to_parquet(gcs_uri, index=False, engine='pyarrow')
        
    except Exception as e:
        print(f"Erro ao salvar dados no Google Cloud Storage: {e}")
        return
    

    # 3. CARREGAMENTO (GCP - BIGQUERY)
    try:
        bigquery_client = bigquery.Client(project=PROJECT_ID)
        
        dataset_ref = bigquery_client.dataset(DATASET_ID)
        bigquery_client.create_dataset(dataset_ref, exists_ok=True)
        print(f"Dataset {DATASET_ID} verificado/criado.")
        
        table_ref = dataset_ref.table(TABLE_ID)
        
        # ****  Carregar do PARQUET (em vez de JSONL) ****
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            # Avisamos ao BigQuery que a fonte é PARQUET
            source_format=bigquery.SourceFormat.PARQUET, 
            autodetect=True, 
        )

        print(f"Carregando dados do GCS ({gcs_uri}) para a tabela {TABLE_ID}...")
        
        load_job = bigquery_client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )
        
        load_job.result() 
        print(f"Carregamento concluído. {load_job.output_rows} linhas carregadas no BigQuery.")
        

    except Exception as e:
        print(f"Erro ao carregar dados no BigQuery: {e}")

if __name__ == "__main__":
    ingest_stock_data()