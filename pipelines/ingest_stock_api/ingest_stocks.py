# pipelines/ingest_stock_api/ingest_stocks.py

import sys
import time
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, to_date, year, month

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================
GCS_BUCKET_NAME = "date_lakehouse_bronze"
GCS_BRONZE_PATH = f"gs://{GCS_BUCKET_NAME}/bronze-stocks/stock_markets"
STOCK_TICKERS = ["IBM", "MSFT", "NVDA"]

try:
    ALPHA_VANTAGE_API_KEY = sys.argv[1]
except IndexError:
    print("❌ ERRO: API Key não fornecida como argumento.")
    sys.exit(1)

# ============================================================================
# FUNÇÕES
# ============================================================================

def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("StockIngestionGCS")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def fetch_stock_data(ticker: str) -> list:
    url = (
        f"https://www.alphavantage.co/query?"
        f"function=TIME_SERIES_DAILY"
        f"&symbol={ticker}"
        f"&outputsize=compact"
        f"&apikey={ALPHA_VANTAGE_API_KEY}"
    )
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json().get('Time Series (Daily)', {})
        result = [
            {
                "ticker": ticker,
                "date": date_str,
                "open": float(v.get("1. open")),
                "high": float(v.get("2. high")),
                "low": float(v.get("3. low")),
                "close": float(v.get("4. close")),
                "volume": int(v.get("5. volume"))
            }
            for date_str, v in data.items()
        ]
        return result
    except Exception as e:
        print(f"❌ Falha na API para {ticker}: {e}")
        return []

def save_to_gcs_delta(df, path: str, mode: str = "append"):
    df_partitioned = (
        df
        .withColumn("ingestion_date", to_date(col("data_ingestao")))
        .withColumn("year", year(col("ingestion_date")))
        .withColumn("month", month(col("ingestion_date")))
    )
    df_partitioned.write.format("delta").mode(mode).partitionBy("ticker","year","month").option("overwriteSchema","true").save(path)

# ============================================================================
# PIPELINE PRINCIPAL
# ============================================================================

def main():
    print("🚀 ETL Bronze Layer - Ações")
    spark = create_spark_session()

    all_stock_data = []
    for ticker in STOCK_TICKERS:
        all_stock_data.extend(fetch_stock_data(ticker))
        time.sleep(15)

    if not all_stock_data:
        # fallback para dados mockados
        all_stock_data = [
            {"ticker": t, "date": "2025-01-01", "open": 100, "high": 110, "low": 90, "close": 105, "volume": 1000}
            for t in STOCK_TICKERS
        ]

    df = spark.createDataFrame(all_stock_data).withColumn("data_ingestao", current_timestamp())
    save_to_gcs_delta(df, GCS_BRONZE_PATH)
    spark.stop()
    print("✅ ETL concluído com sucesso!")

if __name__ == "__main__":
    main()
