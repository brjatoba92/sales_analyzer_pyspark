from src.etl.load_data import criar_spark_session, carregar_dados_vendas, carregar_dados_lojas, carregar_dados_produtos
from src.etl.transform import preparar_dataset_completo
from src.analysis.vendas_por_loja import calcular_agregados_por_loja, gerar_ranking_lojas
from src.analysis.filter import filtrar_por_periodo, filtrar_por_categoria, filtrar_por_subcategoria, filtra_por_regiao
from src.etl.export import exportar_para_csv
from src.visualization.graficos_vendas import plot_ranking_lojas


def main():
    # 1. Iniciar Spark
    spark = criar_spark_session()

    # 2. Carregar dados
    df_vendas = carregar_dados_vendas(spark)
    df_lojas = carregar_dados_lojas(spark)
    df_produtos = carregar_dados_produtos(spark)

    # 3. Preparar dataset completo
    df_completo = preparar_dataset_completo(df_vendas, df_lojas, df_produtos)

    # 4. Aplicar filtros (ajuste conforme necessário)
    df_filtrado = filtrar_por_periodo(df_completo, "2024-01-15", "2024-02-15")
    df_filtrado = filtrar_por_categoria(df_filtrado, "Construção")
    # df_filtrado = filtrar_por_subcategoria(df_filtrado, "Tijolo")
    # df_filtrado = filtrar_por_regiao(df_filtrado, "Nordeste")

    # 5. Calcular agregações e ranking
    df_agregado = calcular_agregados_por_loja(df_filtrado)
    df_ranking = gerar_ranking_lojas(df_agregado, top_n=10)

    # 6. Exibir resultado
    print("\n🏆 Ranking de Lojas por Faturamento:")
    df_ranking.show(truncate=False)

    # 7. Exportar resultado
    exportar_para_csv(df_ranking, "ranking_lojas_filtrado.csv")

    #8. Plotar grafico
    plot_ranking_lojas()

if __name__ == "__main__":
    main()
