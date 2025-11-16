"""
ETL Bronze Layer - Criptomoedas (Cloud-Native com PySpark)
Salva DIRETAMENTE no GCS em formato Delta Lake.
"""
import sys
import json
import time
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, to_date, year, month

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================
GCS_BUCKET_NAME = "date_lakehouse_bronze"
GCS_BRONZE_PATH = f"gs://{GCS_BUCKET_NAME}/bronze-crypto/crypto_markets"
CRYPTO_IDS = [
    "bitcoin","ethereum","solana","cardano","ripple",
    "polkadot","dogecoin","avalanche-2","chainlink","matic-network"
]
API_URL_TEMPLATE = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=brl"
    "&ids={ids}"
    "&order=market_cap_desc&per_page=10&sparkline=false"
)
MAX_RETRIES = 3
RETRY_DELAY = 10  # segundos

# ============================================================================
# FUNÇÕES
# ============================================================================

def create_spark_session() -> SparkSession:
    """Cria a sessão Spark com Delta configurado"""
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
    """Busca dados da API CoinGecko com retry"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"\n📡 Tentativa {attempt}: buscando dados da API CoinGecko...")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            if not data or not isinstance(data, list):
                print(f"⚠️  API retornou dados inválidos")
                return []
            print(f"✅ {len(data)} criptomoedas coletadas")
            return data
        except Exception as e:
            print(f"❌ Falha na API: {e}")
            if attempt < MAX_RETRIES:
                print(f"Pausando {RETRY_DELAY}s antes da próxima tentativa...")
                time.sleep(RETRY_DELAY)
            else:
                return []

def save_to_gcs_delta(df, path: str, mode: str = "append"):
    """Salva DataFrame como Delta Table DIRETAMENTE no GCS"""
    print(f"\n💾 Salvando dados no GCS (Delta Lake)...")
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
        url = API_URL_TEMPLATE.format(ids=",".join(CRYPTO_IDS))
        crypto_data = fetch_crypto_data(url)
        if not crypto_data:
            raise Exception("Falha ao obter dados da API")

        json_strings = [json.dumps(record) for record in crypto_data]
        rdd = spark.sparkContext.parallelize(json_strings)
        df = spark.read.json(rdd)
        df = df.withColumn("data_ingestao", current_timestamp())

        if not save_to_gcs_delta(df, GCS_BRONZE_PATH, mode="append"):
            raise Exception("Falha ao salvar no GCS")

        print("\n✅ ETL CONCLUÍDO COM SUCESSO!")
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {e}")
        sys.exit(1)
    finally:
        if spark:
            print("\n🛑 Encerrando Spark...")
            spark.stop()

if __name__ == "__main__":
    main()
