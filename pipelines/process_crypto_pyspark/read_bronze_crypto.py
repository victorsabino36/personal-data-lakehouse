import os
import sys

# Aponta o Spark para o executável Python correto (o do venv)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from delta import *

# Caminho de onde os dados foram salvos
BRONZE_PATH = "data/bronze/crypto_markets"



def main():
    print("Iniciando leitor da Camada Bronze (3 moedas)...")

    # --- 1. Iniciar Sessão Spark com Delta ---
    builder = (
        SparkSession.builder.appName("ReadBronzeCrypto")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.host", "127.0.0.1")  # <-- ADICIONE ESTA LINHA
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # --- 2. Ler a Delta Table ---
    print(f"Lendo dados da Delta Table em: {BRONZE_PATH}")
    
    df = spark.read.format("delta").load(BRONZE_PATH)

    # --- 3. Mostrar os Dados ---
    print(f"Total de registros na tabela: {df.count()}") # Deve mostrar 3

    print("Mostrando os 3 registros (formato vertical):")
    
    # Usamos vertical=True para uma visualização limpa no terminal
    df.show(truncate=False, vertical=True)
    
    spark.stop()

if __name__ == "__main__":
    main()