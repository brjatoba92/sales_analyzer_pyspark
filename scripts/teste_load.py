from src.etl.load_data import criar_spark_session, carregar_dados_vendas, carregar_dados_lojas, carregar_dados_produtos
from src.etl.transform import preparar_dataset_completo
from src.analysis.vendas_por_loja import calcular_agregados_por_loja, gerar_ranking_lojas

spark = criar_spark_session()
# ETL: Carregar os dados
df_vendas = carregar_dados_vendas(spark)
df_lojas = carregar_dados_lojas(spark)
df_produtos = carregar_dados_produtos(spark)

df_completo = preparar_dataset_completo(df_vendas, df_lojas, df_produtos)

# Análise: Calcular agregados por loja
df_agregado = calcular_agregados_por_loja(df_completo)
# Análise: Gerar ranking das lojas
df_ranking = gerar_ranking_lojas(df_agregado)

# Exibir
df_completo.show(5, truncate=False)


