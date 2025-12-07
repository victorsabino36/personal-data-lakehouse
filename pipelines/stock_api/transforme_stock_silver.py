import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, trim, upper, current_timestamp,
    from_utc_timestamp, max as spark_max, lit, year, month, round
)
from delta.tables import DeltaTable

# ============================================================================
# CONFIGURAÇÕES DA CAMADA SILVER
# ============================================================================
GCS_BUCKET_NAME = "personal-date-lakehouse" 
GCS_BRONZE_PATH = f"gs://{GCS_BUCKET_NAME}/bronze/stock_favorites"
GCS_SILVER_PATH = f"gs://{GCS_BUCKET_NAME}/silver/stock_favorites" 


# Configurações BigQuery
BQ_PROJECT = "personal-data-lakehouse"
BQ_DATASET = "silver_stock"
BQ_TABLE = "stock_favorites"
BQ_FULL_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
TEMP_BUCKET = "dataproc-staging-us-central1-1015215278127-eh5ygvtr"


def create_spark_session() -> SparkSession:
    """Cria a sessão Spark com Delta configurado"""
    # (Função já definida, mantida para contexto)
    print("🚀 Iniciando Spark Session (Dataproc)...")
    builder = (
        SparkSession.builder
        .appName("CryptoRefinementSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"✅ Spark {spark.version} iniciado!")
    return spark
    
def get_max_date_from_bq(spark: SparkSession, table: str) -> str:
    """Consulta o BigQuery para obter a data máxima (global) do pregão (data_pregao)."""
    print(f"\n🔍 Buscando data máxima GLOBAL em {table}...") 
    
    query = f"SELECT MAX(data_pregao) AS max_date FROM `{table}`"   # <-- Correto!

    try:
        df_max_date = spark.read \
        .format("bigquery") \
        .option("project", "personal-data-lakehouse") \
        .option("dataset", "silver_stock") \
        .option("table", "stock_favorites") \
        .load()

        # Calcula a data máxima da coluna 'data_pregao'
        max_date_df = df_max_date.select(spark_max(col("data_pregao")).alias("max_date")) 
        
        # Coleta o resultado
        result = max_date_df.collect()[0]["max_date"]

        if result:
            return result.strftime("%Y-%m-%d")
        else:
            print("⚠️ Tabela vazia. Usando data inicial 1900-01-01.")
            return "1900-01-01"

    except Exception as e:
        print(f"❌ ERRO ao consultar BigQuery: {e}")
        return "1900-01-01"



def read_bronze_data(spark: SparkSession, path: str, max_date_processed: str):
    """Lê os dados brutos da Delta Table (Camada Bronze) aplicando filtro global simples."""
    print(f"\n📚 Lendo dados da Camada Bronze: {path} Filtrando dados com 'date' > {max_date_processed}")


    try:
        # 1. Leitura do Bronze
        df_filtered = (
            spark.read.format("delta").load(path)
            .withColumn("date", to_date(col("date"))) 
            .filter(col("date") > lit(max_date_processed)) 
        )

        num_new_records = df_filtered.count()
        print(f"✅ Registros novos para processar: {num_new_records}")
        
        if num_new_records == 0:
            return None 

        # Retorna o DataFrame limpo
        return df_filtered
        
    except Exception as e:
        print(f"❌ Erro ao ler a Camada Bronze: {e}")
        return None




    
def transform_to_silver(df_bronze):
    """
    Aplica limpeza, padronização e refino para a Camada Silver.
    """
    print("\n⚙️  Iniciando transformações para a Camada Silver...")

    df_silver = (
        df_bronze
        # --- Padronização de colunas ---
        .withColumn("ticker", upper(trim(col("ticker"))))
        
        # --- Convertendo data da API ---
        .withColumn("data_pregao", to_date(col("date")))

        # --- Casting explícito para evitar problemas no Delta ---
        .withColumn("open", col("open").cast("decimal(30, 8)"))
        .withColumn("high", col("high").cast("decimal(30, 8)"))
        .withColumn("low", col("low").cast("decimal(30, 8)"))
        .withColumn("close", col("close").cast("decimal(30, 8)"))
        .withColumn("volume", col("volume").cast("decimal(30, 8)"))
        .withColumn(
            "data_refino",
            from_utc_timestamp(current_timestamp(), "America/Sao_Paulo")
        )
        .drop("date")
    )

    # --- Ordenação opcional ---
    df_silver = df_silver.orderBy("ticker", "data_pregao")

    print("✅ Transformação Silver concluída!")
    return df_silver

def write_silver_data(df_silver, path: str, mode: str = "overwrite"): 
    """Salva o DataFrame refinado como Delta Table na Camada Silver"""
    print(f"\n💾 Salvando dados na Camada Silver: {path}")
    try:
        (
            df_silver.withColumn("year", year(col("data_pregao"))) 
                     .withColumn("month", month(col("data_pregao"))) 
            .write
            .format("delta")
            .mode(mode)
            .option("mergeSchema", "true") 
            .partitionBy("ticker", "year", "month") 
            .save(path) 
        )
        print("✅ Dados salvos com sucesso na Camada Silver!")
        return True
    except Exception as e:
        print(f"❌ Erro ao salvar na Camada Silver: {e}")
        return False

def write_silver_bigquery(df_silver, table: str, temp_bucket: str, mode: str = "overwrite"):
    """Salva o DataFrame refinado no BigQuery"""
    print(f"\n💾 Salvando dados no BigQuery: {table}")
    try:
        (
            df_silver.write
            .format("bigquery")
            .option("project", BQ_PROJECT) 
            .option("writeMethod", "direct")
            .option("temporaryGcsBucket", temp_bucket)
            .mode(mode)
            .save(table)
        )
        print("✅ Dados salvos com sucesso no BigQuery!")
        return True
    except Exception as e:
        print(f"❌ Erro ao salvar no BigQuery: {e}")
        return False

# ============================================================================
# PIPELINE SILVER PRINCIPAL
# ============================================================================
def silver_main():
    print("\n" + "="*80)
    print("⚙️  PIPELINE SILVER LAYER - REFINO")
    print(f"☁️  Destino Silver: {GCS_SILVER_PATH}")
    
    spark = None
    try:
        spark = create_spark_session()
        
        # 1. Determina o ponto de corte incremental (data máxima por ticker)
        df_max_dates = get_max_date_from_bq(spark, BQ_FULL_TABLE)

        # 2. Leitura Incremental da Bronze usando o DF de datas máximas
        df_bronze = read_bronze_data(spark, GCS_BRONZE_PATH, df_max_dates)

        if df_bronze is None:
            raise Exception("Não foi possível ler os dados da Camada Bronze.")

        # Transformação
        df_silver = transform_to_silver(df_bronze)
        
        # 4. Gravação Incremental no Delta Lake (Silver)
        if not write_silver_data(df_silver, GCS_SILVER_PATH, mode="overwrite"):
            raise Exception("Falha ao salvar os dados na Camada Silver (Delta Lake).")

        # 5. Gravação Incremental no BigQuery  
        if not write_silver_bigquery(df_silver, BQ_FULL_TABLE, TEMP_BUCKET, mode="append"):
            raise Exception("Falha ao salvar os dados no BigQuery.")

        print("\n✅ REFINO DA CAMADA SILVER CONCLUÍDO COM SUCESSO!")
        
    except Exception as e:
        print(f"\n❌ ERRO FATAL NO SILVER: {e}")
        sys.exit(1)
    finally:
        if spark:
            print("\n🛑 Encerrando Spark...")
            spark.stop()

if __name__ == "__main__":
    # Para rodar este script separadamente
    silver_main()
