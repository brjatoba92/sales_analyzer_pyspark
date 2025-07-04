from src.etl.load_data import criar_spark_session, carregar_dados_vendas, carregar_dados_lojas, carregar_dados_produtos
from src.etl.transform import preparar_dataset_completo
from src.analysis.vendas_por_loja import calcular_agregados_por_loja, gerar_ranking_lojas
from src.analysis.filter import filtrar_por_periodo, filtrar_por_categoria, filtrar_por_subcategoria, filtra_por_regiao
from src.etl.export import exportar_para_csv

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


# Filtros
"""
Filtros para análise de vendas:
1. Por período: Filtra as vendas dentro de um intervalo de datas.
2. Por categoria: Filtra as vendas de produtos de uma categoria específica.
3. Por subcategoria: Filtra as vendas de produtos de uma subcategoria específica.
4. Por região: Filtra as vendas de lojas em uma região específica.

"""
df_filtrado = filtrar_por_periodo(df_completo, "2024-01-15", "2024-02-15")

df_filtrado = filtrar_por_categoria(df_completo, "Ferragens")

df_filtrado = filtrar_por_subcategoria(df_completo, "Pregos")

df_filtrado = filtra_por_regiao(df_completo, "Nordeste")

# Analise com os dados filtrados
df_agregado_filtrado = calcular_agregados_por_loja(df_filtrado)
df_ranking_filtrado = gerar_ranking_lojas(df_agregado_filtrado)

# Exibir os dados filtrados e o ranking
df_ranking_filtrado.show(10, truncate=False)

# Raking final após filtro e agregação
exportar_para_csv(df_ranking_filtrado)
