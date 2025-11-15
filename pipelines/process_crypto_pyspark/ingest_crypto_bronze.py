"""
ETL Bronze Layer - Criptomoedas (Cloud-Native com PySpark)
Salva DIRETAMENTE no GCS em formato Delta Lake.
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
GCS_BRONZE_PATH = f"gs://{GCS_BUCKET_NAME}/bronze-crypto/crypto_markets"
API_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=brl"
    "&ids=bitcoin,ethereum,solana,cardano,ripple,polkadot,dogecoin,avalanche-2,chainlink,matic-network"
    "&order=market_cap_desc&per_page=10&sparkline=false"
)

# ============================================================================
# FUNÇÕES
# ============================================================================

def create_spark_session() -> SparkSession:
    """Cria a sessão Spark."""
    print("🚀 Iniciando Spark Session (Dataproc)...")
    builder = (
        SparkSession.builder
        .appName("CryptoIngestionGCS")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"✅ Spark {spark.version} iniciado!")
    return spark

def fetch_crypto_data(url: str) -> list:
    """Busca dados da API CoinGecko"""
    print(f"\n📡 Buscando dados da API CoinGecko...")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        if not data or not isinstance(data, list):
            print(f"⚠️  API retornou dados inválidos")
            return None
        print(f"✅ {len(data)} criptomoedas coletadas")
        return data
    except Exception as e:
        print(f"❌ Falha na API: {e}")
        return None

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
            .partitionBy("year", "month")
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
    print("🚀 ETL BRONZE LAYER - CRIPTOMOEDAS (Cloud-Native GCS)")
    print(f"☁️  Destino GCS: {GCS_BRONZE_PATH}")
    
    spark = None
    try:
        spark = create_spark_session()
        json_data = fetch_crypto_data(API_URL)
        if not json_data:
            raise Exception("Falha ao obter dados da API")
        
        json_strings = [json.dumps(record) for record in json_data]
        rdd = spark.sparkContext.parallelize(json_strings)
        df = spark.read.json(rdd)
        df = df.withColumn("data_ingestao", current_timestamp())
        
        if not save_to_gcs_delta(df, GCS_BRONZE_PATH, mode="append"):
            raise Exception("Falha ao salvar no GCS")
        
        print("\n✅ ETL CONCLUÍDO COM SUCESSO!")
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {e}")
        sys.exit(1) # Falha o job
    finally:
        if spark:
            print("\n🛑 Encerrando Spark...")
            spark.stop()

if __name__ == "__main__":
    main()