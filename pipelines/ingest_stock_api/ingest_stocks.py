import os
import requests
from google.cloud import bigquery
from dotenv import load_dotenv
import time # Vamos adicionar o 'time' para pausar entre as APIs

# --- Configuração ---
load_dotenv() 
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json" 

PROJECT_ID = "personal-data-lakehouse" 
DATASET_ID = "stock_bronze"


TABLE_ID = "raw_stock_daily" 


STOCK_TICKERS = ["IBM", "MSFT", "NVDA"] # (IBM, Microsoft, Nvidia)

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

if not ALPHA_VANTAGE_API_KEY:
    raise ValueError("Chave ALPHA_VANTAGE_API_KEY não encontrada. Verifique seu arquivo .env")

def ingest_stock_data():
    """
    Busca dados da API da Alpha Vantage para MÚLTIPLOS tickers 
    e carrega os resultados em UMA ÚNICA tabela no BigQuery.
    """
    print(f"Iniciando ingestão para os tickers: {STOCK_TICKERS}")
    
    
    todos_os_dados = []

   
    for ticker in STOCK_TICKERS:
        print(f"\nBuscando dados para: {ticker}")
        
        # 1. INGESTÃO (API)
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
                # A API gratuita tem um limite de 5 chamadas/minuto. 
                print(f"Nenhum dado encontrado para {ticker}. Resposta da API: {data}")
                time.sleep(15) # Pausa de 15s se a API falhar 
                continue 

            
          
            for data_str, valores in time_series_data.items():
                registro = {
                    "ticker": ticker, 
                    "date": data_str,
                    "open": valores.get("1. open"),
                    "high": valores.get("2. high"),
                    "low": valores.get("3. low"),
                    "close": valores.get("4. close"),
                    "volume": valores.get("5. volume"),
                }
                todos_os_dados.append(registro)

            print(f"API retornou {len(time_series_data)} registros diários para {ticker}.")
            
            # A API gratuita é limitada. Pausamos 15s entre cada chamada 
            print("Pausando 15s para evitar o rate limit da API...")
            time.sleep(15) 

        except requests.exceptions.RequestException as e:
            print(f"Erro ao chamar a API para {ticker}: {e}")
            continue
        except KeyError as e:
            print(f"Erro ao processar o JSON de {ticker}. Erro: {e}")
            continue
        except Exception as e:
            print(f"Um erro inesperado ocorreu na ingestão de {ticker}: {e}")
            continue

    # 2. CARREGAMENTO (GCP - BIGQUERY)
   
    if not todos_os_dados:
        print("Nenhum dado foi coletado. Encerrando.")
        return
        
    try:
        client = bigquery.Client(project=PROJECT_ID)
        
        dataset_ref = client.dataset(DATASET_ID)
        client.create_dataset(dataset_ref, exists_ok=True)
        
        table_ref = dataset_ref.table(TABLE_ID)
        
        job_config = bigquery.LoadJobConfig(
            
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True, 
        )

        print(f"\nCarregando dados na tabela {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}...")
        job = client.load_table_from_json(
            todos_os_dados, # <-- Carrega a lista COMPLETA
            table_ref,
            job_config=job_config
        )
        
        job.result() 
        print(f"Carregamento concluído. {job.output_rows} linhas carregadas no BigQuery.")

    except Exception as e:
        print(f"Erro ao carregar dados no BigQuery: {e}")

if __name__ == "__main__":
    ingest_stock_data()