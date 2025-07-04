from pyspark.sql import DataFrame
from pyspark.sql.functions import sum as _sum, round, col

def calcular_agregados_por_loja(df: DataFrame) -> DataFrame:
    return df.groupBy("loja_id", "nome_loja").agg(
        _sum("total_venda").alias("faturamento_total"),
        _sum("quantidade").alias("total_itens"),
        round(_sum("total_venda") / _sum("quantidade"), 2).alias("ticket_medio")
    )

def gerar_ranking_lojas(df_agregado: DataFrame, top_n: int = 10) -> DataFrame:
    return df_agregado.orderBy(col("faturamento_total").desc()).limit(top_n)
