import os
import sys
import requests
import json 

# Aponta o Spark para o executável Python correto (o do venv)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from delta import *

# --- 1. Configuração (AQUI ESTÁ A MUDANÇA) ---

# URL antiga (top 100):
# API_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=brl&order=market_cap_desc&per_page=100&page=1"

# URL Nova (apenas as 3 moedas):
API_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=brl&ids=bitcoin,ethereum,solana"

BRONZE_PATH = "data/bronze/crypto_markets"

def fetch_crypto_data(url):
    """Busca dados da API e retorna o JSON."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar dados da API: {e}")
        return None

def main():
    print("Iniciando ingestão de criptomoedas (FILTRADO: BTC, ETH, SOL)...")

    # --- 2. Iniciar Sessão Spark com suporte a Delta Lake ---
    builder = (
        SparkSession.builder.appName("CryptoIngestionBronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.host", "127.0.0.1")  # <-- ADICIONE ESTA LINHA
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # --- 3. Buscar Dados da API (Python) ---
    json_data = fetch_crypto_data(API_URL)

    if not json_data:
        print("Nenhum dado retornado da API. Encerrando.")
        spark.stop()
        return

    # --- 4. Carregar Dados no PySpark (Corrigido) ---
    json_data_strings = [json.dumps(record) for record in json_data]
    rdd = spark.sparkContext.parallelize(json_data_strings)
    df = spark.read.json(rdd)

    # Adicionar metadados de ingestão
    df_with_metadata = df.withColumn("data_ingestao", current_timestamp())

    print("Schema dos dados recebidos (agora só 3 moedas):")
    df_with_metadata.printSchema()

    # --- 5. Salvar na Camada Bronze (Lakehouse) ---
    print(f"Salvando dados em {BRONZE_PATH}...")
    
    # Vamos manter o "overwrite" para limpar os 100 registros
    # antigos e salvar apenas os 3 novos.
    (
        df_with_metadata.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(BRONZE_PATH)
    )

    print("Ingestão (filtrada) concluída com sucesso!")
    
    # Vamos mostrar o que foi salvo
    print("Dados salvos na Camada Bronze:")
    df_with_metadata.show(vertical=True)
    
    spark.stop()


if __name__ == "__main__":
    main()