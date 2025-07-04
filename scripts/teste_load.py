from src.etl.load_data import criar_spark_session, carregar_dados_vendas, carregar_dados_lojas, carregar_dados_produtos
from src.etl.transform import preparar_dataset_completo

spark = criar_spark_session()

df_vendas = carregar_dados_vendas(spark)
df_lojas = carregar_dados_lojas(spark)
df_produtos = carregar_dados_produtos(spark)

df_completo = preparar_dataset_completo(df_vendas, df_lojas, df_produtos)
df_completo.show(5, truncate=False)
