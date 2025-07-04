from pyspark.sql import SparkSession
from src.config import paths

def criar_spark_session(app_name="AnaliseVendas"):
    """
    Cria uma sessão Spark.
    """
    spark = SparkSession.builder \
        .appName("ETL Process") \
        .getOrCreate()
    return spark

def carregar_dados_vendas(spark):
    """
    Carrega oo dataset de vendas a partir do arquivo Parquet
    """
    return spark.read.parquet(str(paths.DATA_RAW_DIR / "vendas.parquet"))

def carregar_dados_lojas(spark):
    return spark.read.parquet(str(paths.DATA_RAW_DIR / "lojas.parquet"))

def carregar_dados_produtos(spark):
   return spark.read.parquet(str(paths.DATA_RAW_DIR / "produtos.parquet"))