import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, round

# ============================================================================
# CONFIGURAÇÕES DA CAMADA SILVER
# ============================================================================
GCS_BUCKET_NAME = "date_lakehouse_bronze" 
GCS_BRONZE_PATH = f"gs://{GCS_BUCKET_NAME}/bronze-crypto/crypto_markets"
GCS_SILVER_PATH = f"gs://{GCS_BUCKET_NAME}/silver-crypto/crypto_refined" 

# Configurações BigQuery
BQ_PROJECT = "personal-data-lakehouse"
BQ_DATASET = "silver_crypto"
BQ_TABLE = "crypto_favorites"
BQ_FULL_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
TEMP_BUCKET = "dataproc-staging-us-central1-1015215278127-eh5ygvtr"

# ============================================================================
# FUNÇÕES DA CAMADA SILVER
# ============================================================================

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

def read_bronze_data(spark: SparkSession, path: str):
    """Lê os dados brutos da Delta Table (Camada Bronze)"""
    print(f"\n📚 Lendo dados da Camada Bronze: {path}")
    try:
        df = spark.read.format("delta").load(path)
        print(f"✅ Dados lidos. Total de registros: {df.count()}")
        return df
    except Exception as e:
        print(f"❌ Erro ao ler a Camada Bronze: {e}")
        return None

def transform_to_silver(df_bronze):
    """Aplica limpeza, padronização e refino para a Camada Silver"""
    print("\n⚙️  Iniciando transformações para a Camada Silver...")
    
    # 1. Seleção, Renomeação e Casting de Tipos (Limpeza)
    df_silver = (
        df_bronze
        .select(
            # Renomeia ID
            col("id").alias("crypto_id"),
            col("symbol").alias("simbolo"),
            col("name").alias("nome"),
            
            # Padroniza e Converte Tipos Numéricos
            col("current_price").cast("decimal(20, 4)").alias("preco_atual_brl"),
            col("market_cap").cast("bigint").alias("capitalizacao_mercado"),
            col("total_volume").cast("bigint").alias("volume_total_24h"),
            round(col("price_change_percentage_24h"), 4).alias("variacao_preco_24h_perc"),
            col("circulating_supply").cast("decimal(30, 8)").alias("oferta_circulante"),
            
            # Seleciona Metadados de Ingestão
            col("data_ingestao") # Coluna de timestamp RAW
        )
        # 2. Remoção de Registros com Preço Nulo (Qualidade)
        .filter(col("preco_atual_brl").isNotNull())
        # 3. Enriquecimento (Adiciona Timestamp Silver)
        .withColumn("data_refino", current_timestamp())
    )
    
    print("✅ Transformações da Camada Silver concluídas.")
    return df_silver

def write_silver_data(df_silver, path: str, mode: str = "overwrite"):
    """Salva o DataFrame refinado como Delta Table na Camada Silver"""
    print(f"\n💾 Salvando dados na Camada Silver: {path}")
    try:
        (
            df_silver.write
            .format("delta")
            .mode(mode)
            .option("mergeSchema", "true") # Permite adição segura de novas colunas
            # A camada Silver não costuma usar as mesmas partições da Bronze
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
    print("⚙️  PIPELINE SILVER LAYER - REFINO DE CRIPTOMOEDAS")
    print(f"☁️  Destino Silver: {GCS_SILVER_PATH}")
    
    spark = None
    try:
        spark = create_spark_session()
        
        # Leitura
        df_bronze = read_bronze_data(spark, GCS_BRONZE_PATH)
        if df_bronze is None:
            raise Exception("Não foi possível ler os dados da Camada Bronze.")

        # Transformação
        df_silver = transform_to_silver(df_bronze)
        
        # Gravação
        if not write_silver_data(df_silver, GCS_SILVER_PATH, mode="overwrite"):
            raise Exception("Falha ao salvar os dados na Camada Silver.")

        # Gravação no BigQuery  
        if not write_silver_bigquery(df_silver, BQ_FULL_TABLE, TEMP_BUCKET, mode="overwrite"):
            raise Exception("Falha ao salvar os dados no BigQuery.")

        print("\n✅ REFINO DA CAMADA SILVER CONCLUÍDO COM SUCESSO!")
        print("✅ Dados disponíveis em Delta Lake (GCS) e BigQuery!") 
        
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