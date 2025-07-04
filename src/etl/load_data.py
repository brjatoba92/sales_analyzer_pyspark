from pyspark.sql import SparkSession
from src.config.paths import RAW_DATA_DIR

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
    path = RAW_DATA_DIR / "vendas.parquet"
    return spark.read.parquet(str(path))

def carregar_dados_lojas(spark):
    """
    Carrega o dataset de lojas a partir do arquivo Parquet
    """
    path = RAW_DATA_DIR / "lojas.parquet"
    return spark.read.parquet(str(path))

def carregar_dados_produtos(spark):
    """
    Carrega o dataset de produtos a partir do arquivo Parquet
    """
    path = RAW_DATA_DIR / "produtos.parquet"
    return spark.read.parquet(str(path))
