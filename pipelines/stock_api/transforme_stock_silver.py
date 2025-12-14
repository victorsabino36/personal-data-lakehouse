import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, trim, upper, current_timestamp,
    from_utc_timestamp, max as spark_max, lit, year, month, round
)
from delta.tables import DeltaTable
import traceback
from google.cloud import bigquery
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

# Nome da tabela temporária no BigQuery para o MERGE
BQ_STAGING_TABLE = "stock_favorites_staging"
BQ_FULL_STAGING_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_STAGING_TABLE}"


def create_spark_session() -> SparkSession:
    """Cria a sessão Spark com Delta e BigQuery configurados"""
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
    
    # CONFIGURAÇÃO GLOBAL DO BIGQUERY (Conforme documentação oficial)
    spark.conf.set('temporaryGcsBucket', TEMP_BUCKET)
    
    print(f"✅ Spark {spark.version} iniciado!")
    print(f"✅ BigQuery temporaryGcsBucket configurado: {TEMP_BUCKET}")
    return spark

    
def get_max_date_df_from_bq(spark: SparkSession, table: str):
    """
    Lê a tabela do BigQuery usando .load() (igual ao exemplo oficial)
    e retorna a data máxima por ticker.
    """
    print(f"\n📥 Lendo dados do BigQuery para determinar ponto de corte incremental: {table}")

    # Bucket temporário (igual ao exemplo)
    bucket = "dataproc-staging-us-central1-1015215278127-eh5ygvtr"
    spark.conf.set("temporaryGcsBucket", bucket)

    # 🔹 Load data from BigQuery (IDÊNTICO AO EXEMPLO)
    df = (
        spark.read
            .format("bigquery")
            .load(table)   # ← project:dataset.table
    )

    # 🔹 Cria a view temporária (IDÊNTICO AO EXEMPLO)
    df.createOrReplaceTempView("stock_favorites")

    # 🔹 Operação via Spark SQL (como no exemplo)
    df_max_date_per_ticker = spark.sql("""
        SELECT
            ticker,
            MAX(data_pregao) AS max_date
        FROM stock_favorites
        GROUP BY ticker
    """)

    print("\n📊 Amostra das Datas Máximas (Ponto de Corte Ticker | Data):")
    df_max_date_per_ticker.show(n=5, truncate=False)

    return df_max_date_per_ticker



def read_bronze_data(spark: SparkSession, path: str, df_max_dates):
    """
    Lê os dados brutos da Delta Table (Camada Bronze) aplicando um filtro 
    incremental por ticker e data, usando o DataFrame de datas máximas.
    """
    print(f"\n📚 Lendo dados da Camada Bronze: {path} aplicando filtro Incremental por Ticker.")
    
    # 1. Leitura Completa da Bronze (já particionada)
    df_bronze = spark.read.format("delta").load(path)
    df_bronze = df_bronze.withColumn("date", to_date(col("date"))) 
    df_bronze = df_bronze.withColumn("ticker", upper(trim(col("ticker"))))

    df_bronze.createOrReplaceTempView("bronze_data_view")
    
    # 2. Executa a agregação MAX(date) via Spark SQL usando a View
    df_data_max_bronze = spark.sql("""
        SELECT 
            ticker, 
            MAX(date) AS max_date 
        FROM bronze_data_view 
        GROUP BY ticker
    """)

    print("\n📊 Amostra das Datas Máximas na Camada Bronze (Diagnóstico):")
    # Exibe as datas máximas que o pipeline da Bronze possui
    df_data_max_bronze.show(n=5, truncate=False)

    # 2. Se o DF de datas máximas (BQ) estiver vazio, processamos tudo
    if df_max_dates.isEmpty():
        num_new_records = df_bronze.count()
        print(f"✅ Tabela BQ vazia. Registros novos para processar (FULL LOAD): {num_new_records}")
        return df_bronze
        
    # 3. Join e Filtro Incremental Robusto
    df_max_dates = df_max_dates.withColumnRenamed("max_date", "bq_max_date").alias("max_dates_bq")
    
    # Faz o JOIN entre Bronze e o DataFrame de datas máximas do BQ
    df_joined = df_bronze.join(
        df_max_dates,
        on=["ticker"],
        how="left"
    )
    
    df_filtered = df_joined.filter(
        (col("bq_max_date").isNull()) | 
        (col("date") > col("bq_max_date"))
    ).drop("bq_max_date")

    num_new_records = df_filtered.count()
    print(f"✅ Registros novos para processar (Incremental): {num_new_records}")
    
    if num_new_records == 0:
        return None 

    return df_filtered

    
def transform_to_silver(df_bronze):
    """
    Aplica limpeza, padronização e refino para a Camada Silver.
    """
    print("\n⚙️  Iniciando transformações para a Camada Silver...")

    df_silver = (
        df_bronze
        .withColumn("ticker", upper(trim(col("ticker"))))
        .withColumn("data_pregao", to_date(col("date")))
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
    

def merge_data_in_bigquery(spark: SparkSession, target_table: str, staging_table: str):
    """Executa a operação MERGE SQL no BigQuery."""
    print(f"\n🔄 Executando MERGE de {staging_table} para {target_table}...")

    # Define o nome da tabela temporária (staging)
    staging_full_table = f"{BQ_PROJECT}.{BQ_DATASET}.{staging_table}"

    # Define a chave primária para o MERGE
    merge_key = "t.data_pregao = s.data_pregao AND t.ticker = s.ticker"

    # Comando MERGE SQL
    merge_query = f"""
        MERGE INTO `{target_table}` t
        USING `{staging_full_table}` s
        ON {merge_key}
        WHEN MATCHED THEN
            UPDATE SET 
                t.open = s.open,
                t.high = s.high,
                t.low = s.low,
                t.close = s.close,
                t.volume = s.volume,
                t.data_refino = s.data_refino
        WHEN NOT MATCHED THEN
            INSERT (ticker, data_pregao, open, high, low, close, volume, data_refino) 
            VALUES (s.ticker, s.data_pregao, s.open, s.high, s.low, s.close, s.volume, s.data_refino)
    """

    try:
        # 🔹 CORREÇÃO: Uso do BigQuery Client em vez de Spark.read
        client = bigquery.Client() # Usa as credenciais do ambiente (Service Account)
        query_job = client.query(merge_query) 
        query_job.result()
        
        print("✅ MERGE executado com sucesso no BigQuery!")
        return True
        
    except Exception as e:
        print(f"❌ ERRO ao executar MERGE no BigQuery: {e}")
        traceback.print_exc(file=sys.stdout)
        return False


def write_silver_bigquery_staging(df_silver, table: str):
    """Escreve os dados incrementais na Tabela Staging do BigQuery (modo overwrite)."""
    print(f"\n💾 Salvando dados na Tabela Staging: {table}")
    try:
        # MÉTODO 2: Usando table completo (project.dataset.table)
        (
            df_silver.write
            .format("bigquery")
            .option("writeMethod", "direct")
            .mode("overwrite")
            .save(table)
        )
        print("✅ Dados salvos com sucesso na Staging Table!")
        return True
    except Exception as e:
        print(f"❌ Erro ao salvar na Staging Table: {e}")
        traceback.print_exc(file=sys.stdout)
        return False


# ============================================================================
# PIPELINE SILVER PRINCIPAL
# ============================================================================
def silver_main():
    print("\n" + "="*80)
    print("⚙️  PIPELINE SILVER LAYER - REFINO COM MERGE NO BIGQUERY")
    print(f"☁️  Destino Silver: {GCS_SILVER_PATH}")
    
    spark = None
    try:
        spark = create_spark_session()
        
        # 1. Determina o ponto de corte incremental (DataFrame por Ticker)
        df_max_dates_per_ticker = get_max_date_df_from_bq(spark, BQ_FULL_TABLE)

        # 2. Leitura Incremental da Bronze usando o DF de datas máximas POR TICKER
        df_bronze = read_bronze_data(spark, GCS_BRONZE_PATH, df_max_dates_per_ticker)
        if df_bronze is None:
            print("⚠️ Não há novos registros incrementais para processar. Finalizando.")
            return

        # 3. Transformação
        df_silver = transform_to_silver(df_bronze)
        
        # 4. Gravação Incremental no Delta Lake (Silver)
        if not write_silver_data(df_silver, GCS_SILVER_PATH, mode="append"):
            raise Exception("Falha ao salvar os dados na Camada Silver (Delta Lake).")

        # 5. Gravação na Tabela STAGING do BigQuery (Escrita Incremental)
        if not write_silver_bigquery_staging(df_silver, BQ_FULL_STAGING_TABLE):
            raise Exception("Falha ao salvar os dados na Staging Table.")
        
        # 6. Execução do MERGE SQL (Garantia de Idempotência)
        if not merge_data_in_bigquery(spark, BQ_FULL_TABLE, BQ_STAGING_TABLE):
            raise Exception("Falha na execução do MERGE SQL no BigQuery.")

        print("\n✅ REFINO DA CAMADA SILVER CONCLUÍDO E DADOS MERGEADOS NO BIGQUERY!")
        
    except Exception as e:
        print(f"\n❌ ERRO FATAL NO SILVER: {e}")
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)
    finally:
        if spark:
            print("\n🛑 Encerrando Spark...")
            spark.stop()


if __name__ == "__main__":
    silver_main()