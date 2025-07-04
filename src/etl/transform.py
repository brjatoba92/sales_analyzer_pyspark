from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round as spark_round

def preparar_dataset_completo(df_vendas: DataFrame, df_lojas: DataFrame, df_produtos: DataFrame) -> DataFrame:
    """    
    Prepara o dataset completo unindo vendas, lojas e produtos.
    Calcula o total de vendas por linha.
    Args:
        df_vendas (DataFrame): DataFrame de vendas.
        df_lojas (DataFrame): DataFrame de lojas.
        df_produtos (DataFrame): DataFrame de produtos.
    Returns:
        DataFrame: DataFrame resultante com as colunas de vendas, lojas e produtos unidas, 
                   incluindo o total de vendas por linha.
    """
    df_vendas = df_vendas.withColumn("total_venda", df_vendas["quantidade"] * df_vendas["valor_unitario"])
    df_join = df_vendas.join(df_lojas, on="loja_id", how="left") \
                       .join(df_produtos, on="produto_id", how="left")
    return df_join
    