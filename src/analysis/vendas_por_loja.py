from pyspark.sql import DataFrame
from pyspark.sql.functions import sum as spark_sum, count, avg, round as spark_round


def calcular_agregados_por_loja(df: DataFrame) -> DataFrame:
   """
    Calcula:
    - Total de vendas (valor_total)
    - Quantidade total de itens vendidos
    - Ticket médio (valor_total / quantidade)
    """
   df_agg = df.groupBy("loja_id", "nome_loja", "cidade", "estado", "regiao") \
       .agg(
           spark_round(spark_sum("valor_total"), 2).alias("total_vendas"),
           spark_sum("quantidade").alias("quantidade_total"),
           spark_round(spark_sum("valor_total") / count("quantidade"), 2).alias("ticket_medio")
       )
   return df_agg

def gerar_ranking_lojas(df_agregado: DataFrame, top_n: int = 10) -> DataFrame:
    """
    Gera um ranking das lojas com base no total de vendas.
    Retorna as top N lojas.
    """
    df_rancked = df_agregado.orderBy(df_agregado["total_vendas"].desc())
    return df_rancked.limit(top_n)