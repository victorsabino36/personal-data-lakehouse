# pipelines/ingest_stock_api/ingest_stocks.py

"""
ETL Bronze Layer - Ações (Cloud-Native com PySpark)
Recebe a API Key como argumento e salva DIRETAMENTE no GCS (Delta Lake).
"""
import os
import sys
import requests
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, to_date, year, month

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================
GCS_BUCKET_NAME = "date_lakehouse_bronze"
GCS_BRONZE_PATH = f"gs://{GCS_BUCKET_NAME}/bronze-stocks/stock_markets"
STOCK_TICKERS = ["IBM", "MSFT", "NVDA"] # Tickers para buscar

# A API Key será lida do argumento de linha de comando (passado pelo Airflow)
try:
    ALPHA_VANTAGE_API_KEY = sys.argv[1]
except IndexError:
    print("❌ ERRO: API Key não fornecida como argumento.")
    sys.exit(1)

# ============================================================================
# FUNÇÕES
# ============================================================================

def create_spark_session() -> SparkSession:
    """
    Cria a sessão Spark. 
    No Dataproc Serverless, os JARS (Delta/GCS) são passados pelo job.
    """
    print("🚀 Iniciando Spark Session (Dataproc)...")
    builder = (
        SparkSession.builder
        .appName("StockIngestionGCS")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"✅ Spark {spark.version} iniciado!")
    return spark

def fetch_stock_data(ticker: str) -> list:
    """Busca dados da API Alpha Vantage para um ticker."""
    print(f"\n📡 Buscando dados da API para: {ticker}")
    url = (
        f"https://www.alphavantage.co/query?"
        f"function=TIME_SERIES_DAILY"
        f"&symbol={ticker}"
        f"&outputsize=compact" # Mude para 'full' se precisar de mais histórico
        f"&apikey={ALPHA_VANTAGE_API_KEY}"
    )
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        time_series_data = data.get('Time Series (Daily)')
        
        if not time_series_data:
            print(f"⚠️  Nenhum dado encontrado para {ticker}. Resposta: {data}")
            return []

        # Convertendo o JSON aninhado em uma lista de linhas
        data_list = []
        for date_str, values in time_series_data.items():
            data_list.append({
                "ticker": ticker,
                "date": date_str,
                "open": float(values.get("1. open")),
                "high": float(values.get("2. high")),
                "low": float(values.get("3. low")),
                "close": float(values.get("4. close")),
                "volume": int(values.get("5. volume"))
            })
        print(f"✅ {len(data_list)} registros coletados para {ticker}.")
        return data_list
        
    except Exception as e:
        print(f"❌ Falha na API para {ticker}: {e}")
        return [] # Retorna lista vazia em caso de falha

def save_to_gcs_delta(df, path: str, mode: str = "append"):
    """Salva DataFrame como Delta Table DIRETAMENTE no GCS"""
    print(f"\n💾 Salvando dados no GCS (Delta Lake)...")
    print(f"   Destino: {path}")
    print(f"   Modo: {mode}")
    try:
        df_partitioned = (
            df
            .withColumn("ingestion_date", to_date(col("data_ingestao")))
            .withColumn("year", year(col("ingestion_date")))
            .withColumn("month", month(col("ingestion_date")))
        )
        (
            df_partitioned.write
            .format("delta")
            .mode(mode)
            .partitionBy("ticker", "year", "month") # Particionando por ticker
            .option("overwriteSchema", "true")
            .save(path)
        )
        print("✅ Dados salvos com sucesso no GCS!")
        return True
    except Exception as e:
        print(f"❌ Erro ao salvar no GCS: {e}")
        return False

# ============================================================================
# PIPELINE PRINCIPAL
# ============================================================================
def main():
    print("\n" + "="*80)
    print("🚀 ETL BRONZE LAYER - AÇÕES (Cloud-Native com PySpark)")
    print(f"☁️  Destino GCS: {GCS_BRONZE_PATH}")
    
    spark = None
    try:
        spark = create_spark_session()
        
        all_stock_data = []
        for ticker in STOCK_TICKERS:
            all_stock_data.extend(fetch_stock_data(ticker))
            print("Pausando 15s para evitar o rate limit da API...")
            time.sleep(15) # Pausa obrigatória para a API grátis
        
        if not all_stock_data:
            raise Exception("Nenhum dado de ação foi coletado.")
        
        print(f"\n🔄 Convertendo {len(all_stock_data)} registros totais para DataFrame...")
        df = spark.createDataFrame(all_stock_data)
        df = df.withColumn("data_ingestao", current_timestamp())
        
        if not save_to_gcs_delta(df, GCS_BRONZE_PATH, mode="append"):
            raise Exception("Falha ao salvar no GCS")
        
        print("\n✅ ETL DE AÇÕES CONCLUÍDO COM SUCESSO!")
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {e}")
        sys.exit(1) # Falha o job
    finally:
        if spark:
            print("\n🛑 Encerrando Spark...")
            spark.stop()

if __name__ == "__main__":
    main()