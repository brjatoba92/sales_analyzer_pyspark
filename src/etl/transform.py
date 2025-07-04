from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round as spark_round

def preparar_dataset_completo(df_vendas: DataFrame, df_lojas: DataFrame, df_produtos: DataFrame) -> DataFrame:
    """
    Realiza as transformações necessárias:
    - Cria a coluna valor_total
    - Faz os joins com lojas e produtos
    - Retorna o DataFrame consolidado
    """

    # 1. Adiciona a coluna valor_total
    df_vendas = df_vendas.withColumn("valor_total", col("quantidade") * col("preco_unitario"))

    # 2. Faz o join com a tabela de lojas
    df_join_lojas = df_vendas.join(df_lojas, on="loja_id", how="left")

    # 3. Faz o join com a tabela de produtos
    df_final = df_join_lojas.join(df_produtos, on="produto_id", how="left")
    