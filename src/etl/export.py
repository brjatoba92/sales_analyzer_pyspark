from pyspark.sql import DataFrame
from src.config import paths

def exportar_para_csv(df: DataFrame, nome_arquivo: str):
    output_path = paths.DATA_PROCESSED_DIR / nome_arquivo
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_path))
    print(f"✅ Arquivo salvo em: {output_path}")
