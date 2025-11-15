"""
ETL Bronze Layer - Criptomoedas
Solução SEM JARS: Salva local em Delta Lake e depois faz upload para GCS
"""
import os
import sys
import requests
import json
from datetime import datetime
from google.cloud import storage

# Força o Spark a usar o Python correto
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, lit, col, 
    to_date, year, month, dayofmonth
)
from delta import configure_spark_with_delta_pip

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================
PROJECT_ID = "personal-data-lakehouse"
GCS_BUCKET_NAME = "date_lakehouse_bronze"

# Paths LOCAIS (sem gs://)
LOCAL_BRONZE_PATH = "data/bronze/crypto_markets"
GCS_BRONZE_PREFIX = "bronze-crypto/crypto_markets"  # Prefixo no bucket

# API CoinGecko - Top 10 criptos
API_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=brl"
    "&ids=bitcoin,ethereum,solana,cardano,ripple,polkadot,dogecoin,avalanche-2,chainlink,matic-network"
    "&order=market_cap_desc"
    "&per_page=10"
    "&sparkline=false"
)


# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================
def create_spark_session() -> SparkSession:
    """Cria sessão Spark APENAS para Delta Lake LOCAL"""
    print("🚀 Iniciando Spark Session (modo local)...")
    
    builder = (
        SparkSession.builder
        .appName("CryptoIngestionLocal")
        .master("local[*]")
        
        # Delta Lake Extensions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # Otimizações
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "2g")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark {spark.version} iniciado com sucesso!")
    return spark


def fetch_crypto_data(url: str, retries: int = 3) -> list:
    """Busca dados da API CoinGecko"""
    print(f"\n📡 Buscando dados da API CoinGecko...")
    
    for attempt in range(1, retries + 1):
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
            print(f"❌ Tentativa {attempt}/{retries} falhou: {e}")
            if attempt < retries:
                import time
                time.sleep(5)
    
    return None


def save_to_local_delta(df, path: str, mode: str = "append"):
    """Salva DataFrame como Delta Table LOCALMENTE"""
    print(f"\n💾 Salvando dados localmente...")
    print(f"   Destino: {path}")
    print(f"   Modo: {mode}")
    print(f"   Registros: {df.count()}")
    
    try:
        # Adiciona colunas de particionamento
        df_partitioned = (
            df
            .withColumn("ingestion_date", to_date(col("data_ingestao")))
            .withColumn("year", year(col("ingestion_date")))
            .withColumn("month", month(col("ingestion_date")))
        )
        
        # Salva localmente
        (
            df_partitioned.write
            .format("delta")
            .mode(mode)
            .partitionBy("year", "month")
            .option("overwriteSchema", "true")
            .save(path)
        )
        
        print("✅ Dados salvos localmente com sucesso!")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao salvar localmente: {e}")
        return False


def upload_to_gcs(local_path: str, bucket_name: str, gcs_prefix: str):
    """
    Faz upload da Delta Table local para o GCS
    
    Args:
        local_path: Caminho local da Delta Table
        bucket_name: Nome do bucket GCS
        gcs_prefix: Prefixo no bucket (ex: 'bronze/crypto')
    """
    print(f"\n☁️  Fazendo upload para GCS...")
    print(f"   Bucket: {bucket_name}")
    print(f"   Prefixo: {gcs_prefix}")
    
    try:
        # Inicializa cliente GCS
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(bucket_name)
        
        # Conta arquivos
        total_files = 0
        uploaded_files = 0
        
        # Percorre todos os arquivos locais
        for root, dirs, files in os.walk(local_path):
            for file in files:
                total_files += 1
                local_file = os.path.join(root, file)
                
                # Calcula caminho relativo
                relative_path = os.path.relpath(local_file, local_path)
                gcs_path = f"{gcs_prefix}/{relative_path}"
                
                # Upload
                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(local_file)
                uploaded_files += 1
                
                if uploaded_files % 10 == 0:
                    print(f"   📤 {uploaded_files}/{total_files} arquivos...")
        
        print(f"✅ Upload concluído: {uploaded_files} arquivos enviados!")
        print(f"   URL: gs://{bucket_name}/{gcs_prefix}/")
        return True
        
    except Exception as e:
        print(f"❌ Erro no upload para GCS: {e}")
        print(f"   Verifique se você está autenticado:")
        print(f"   gcloud auth application-default login")
        return False


def read_local_delta(spark, path: str):
    """Lê e mostra estatísticas da Delta Table local"""
    try:
        df = spark.read.format("delta").load(path)
        total = df.count()
        
        print(f"\n📊 Estatísticas da Tabela Local:")
        print(f"   Total de registros: {total}")
        print(f"   Criptos únicas: {df.select('id').distinct().count()}")
        
        # Top 5
        print(f"\n🏆 Top 5 Criptomoedas:")
        (
            df.select("symbol", "name", "current_price", "market_cap_rank")
            .orderBy("market_cap_rank")
            .show(5, truncate=False)
        )
        
    except Exception as e:
        print(f"⚠️  Não foi possível ler tabela local: {e}")


# ============================================================================
# PIPELINE PRINCIPAL
# ============================================================================
def main():
    """Executa o pipeline completo"""
    
    print("\n" + "="*80)
    print("🚀 ETL BRONZE LAYER - CRIPTOMOEDAS (LOCAL + GCS)")
    print("="*80)
    print(f"📅 Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"💾 Destino Local: {LOCAL_BRONZE_PATH}")
    print(f"☁️  Destino GCS: gs://{GCS_BUCKET_NAME}/{GCS_BRONZE_PREFIX}/")
    
    spark = None
    
    try:
        # 1. Cria Spark Session
        spark = create_spark_session()
        
        # 2. Busca dados da API
        json_data = fetch_crypto_data(API_URL)
        
        if not json_data:
            print("\n❌ Falha ao obter dados da API")
            return
        
        # 3. Converte para DataFrame Spark
        print(f"\n🔄 Convertendo {len(json_data)} registros para DataFrame...")
        json_strings = [json.dumps(record) for record in json_data]
        rdd = spark.sparkContext.parallelize(json_strings)
        df = spark.read.json(rdd)
        
        # Adiciona timestamp
        df = df.withColumn("data_ingestao", current_timestamp())
        
        print("✅ DataFrame criado!")
        
        # 4. Salva LOCALMENTE em Delta Lake
        success = save_to_local_delta(df, LOCAL_BRONZE_PATH, mode="append")
        
        if not success:
            print("\n❌ Falha ao salvar localmente")
            return
        
        # 5. Mostra dados locais
        read_local_delta(spark, LOCAL_BRONZE_PATH)
        
        # 6. Upload para GCS
        print("\n" + "="*80)
        upload_success = upload_to_gcs(
            LOCAL_BRONZE_PATH, 
            GCS_BUCKET_NAME, 
            GCS_BRONZE_PREFIX
        )
        
        if upload_success:
            print("\n" + "="*80)
            print("✅ ETL CONCLUÍDO COM SUCESSO!")
            print("="*80)
            print("\n📍 Seus dados estão em:")
            print(f"   1. Local: {LOCAL_BRONZE_PATH}")
            print(f"   2. GCS: gs://{GCS_BUCKET_NAME}/{GCS_BRONZE_PREFIX}/")
            print("\n💡 Para ler do GCS, use:")
            print(f"   gsutil ls gs://{GCS_BUCKET_NAME}/{GCS_BRONZE_PREFIX}/")
        else:
            print("\n⚠️  ETL executado, mas upload para GCS falhou")
            print(f"   Dados salvos localmente em: {LOCAL_BRONZE_PATH}")
        
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if spark:
            print("\n🛑 Encerrando Spark...")
            spark.stop()


if __name__ == "__main__":
    # Cria diretório local se não existir
    os.makedirs(os.path.dirname(LOCAL_BRONZE_PATH), exist_ok=True)
    main()