import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from src.config import paths
import os

def plot_ranking_lojas():
    # Caminho da pasta gerada pelo Spark
    pasta_csv = paths.DATA_PROCESSED_DIR / "ranking_lojas_filtrado.csv"

    # Encontra o arquivo part-*.csv dentro da pasta
    arquivo_csv = None
    for nome in os.listdir(pasta_csv):
        if nome.startswith("part-") and nome.endswith(".csv"):
            arquivo_csv = pasta_csv / nome
            break

    if not arquivo_csv or not arquivo_csv.exists():
        print("❌ Arquivo CSV não encontrado.")
        return

    # Lê o arquivo com pandas
    df = pd.read_csv(arquivo_csv)

    # Ordena corretamente
    df = df.sort_values(by="faturamento_total", ascending=False)

    # Gera o gráfico
    plt.figure(figsize=(10, 6))
    sns.barplot(x="faturamento_total", y="nome_loja", data=df, palette="viridis")

    plt.title("🏆 Ranking de Lojas por Faturamento")
    plt.xlabel("Faturamento Total")
    plt.ylabel("Loja")
    plt.tight_layout()

    # Caminho de saída do gráfico
    output_path = paths.DATA_PROCESSED_DIR / "grafico_ranking_lojas.png"
    plt.savefig(output_path)
    print(f"✅ Gráfico salvo em: {output_path}")
    plt.show()
