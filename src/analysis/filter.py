from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def filtrar_por_periodo(df: DataFrame, data_inicio: str, data_fim: str) -> DataFrame:
    """
    Filtra o DataFrame por um período específico.
    :param df: DataFrame com os dados de vendas.
    :param data_inicio: Data de início no formato 'yyyy-MM-dd'.
    :param data_fim: Data de fim no formato 'yyyy-MM-dd'.
    :return: DataFrame filtrado pelo período.
    """
    return df.filter((col("data") >= data_inicio) & (col("data") <= data_fim))

def filtrar_por_categoria(df: DataFrame, categoria: str) -> DataFrame:
    """
    Filtra o DataFrame por uma categoria específica de produto.
    :param df: DataFrame com os dados de vendas.
    :param categoria: Categoria do produto a ser filtrada.
    :return: DataFrame filtrado pela categoria.
    """
    return df.filter(col("categoria") == categoria)

def filtrar_por_subcategoria(df: DataFrame, subcategoria: str) -> DataFrame:
    """
    Filtra o DataFrame por uma subcategoria específica de produto.
    :param df: DataFrame com os dados de vendas.
    :param subcategoria: Subcategoria do produto a ser filtrada.
    :return: DataFrame filtrado pela subcategoria.
    """
    return df.filter(col("subcategoria") == subcategoria)

def filtra_por_regiao(df: DataFrame, regiao: str) -> DataFrame:
    """
    Filtra o DataFrame por uma região específica.
    :param df: DataFrame com os dados de vendas.
    :param regiao: Região a ser filtrada.
    :return: DataFrame filtrado pela região.
    """
    return df.filter(col("regiao") == regiao)