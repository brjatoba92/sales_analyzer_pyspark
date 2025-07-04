from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def filtrar_por_periodo(df: DataFrame, data_inicio: str, data_fim: str) -> DataFrame:
    return df.filter((col("data") >= data_inicio) & (col("data") <= data_fim))

def filtrar_por_categoria(df: DataFrame, categoria: str) -> DataFrame:
    return df.filter(col("categoria") == categoria)

def filtrar_por_subcategoria(df: DataFrame, subcategoria: str) -> DataFrame:
    return df.filter(col("subcategoria") == subcategoria)

def filtra_por_regiao(df: DataFrame, regiao: str) -> DataFrame:
    return df.filter(col("regiao") == regiao)
