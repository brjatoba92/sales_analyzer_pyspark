from pyspark.sql import DataFrame
from src.config.paths import PROCESSED_DATA_DIR

def exportar_para_csv(df: DataFrame, nome_arquivo: str = "ranking_lojas.csv"):
    """
    Converte o DataFrame PySpark em Pandas e exporta para CSV.
    Args:
        df (DataFrame): DataFrame PySpark a ser exportado.
        nome_arquivo (str): Nome do arquivo CSV de saída. Padrão é "ranking_lojas.csv".
    Retorna:
        None
    Exemplo:
        exportar_dados(df_ranking_lojas, "ranking_lojas.csv")
    """
    caminho_saida = PROCESSED_DATA_DIR / nome_arquivo
    df.toPandas().to_csv(caminho_saida, index=False)
    print(f"Dados exportados para {caminho_saida}")