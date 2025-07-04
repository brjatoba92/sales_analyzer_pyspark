import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Permitir importar o módulo src
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

# Importa o caminho centralizado
from src.config.paths import RAW_DATA_DIR


def gerar_vendas():
    np.random.seed(42)
    datas = pd.date_range(start="2024-01-01", periods=100, freq="D")
    vendas = pd.DataFrame({
        "data": np.random.choice(datas, size=200),
        "loja_id": np.random.choice([1, 2, 3], size=200),
        "produto_id": np.random.choice([101, 102, 103, 104], size=200),
        "quantidade": np.random.randint(1, 10, size=200),
        "valor_unitario": np.random.uniform(10, 100, size=200).round(2)
    })
    vendas.to_parquet(RAW_DATA_DIR / "vendas.parquet", index=False)
    print("✔ vendas.parquet gerado.")


def gerar_lojas():
    lojas = pd.DataFrame({
        "loja_id": [1, 2, 3],
        "nome_loja": ["Loja Norte", "Loja Sul", "Loja Centro"],
        "cidade": ["Maceió", "Arapiraca", "Palmeira dos Índios"],
        "estado": ["AL", "AL", "AL"],
        "regiao": ["Nordeste", "Nordeste", "Nordeste"]
    })
    lojas.to_parquet(RAW_DATA_DIR / "lojas.parquet", index=False)
    print("✔ lojas.parquet gerado.")


def gerar_produtos():
    produtos = pd.DataFrame({
        "produto_id": [101, 102, 103, 104],
        "nome_produto": ["Cimento", "Areia", "Tijolo", "Prego"],
        "categoria": ["Construção", "Construção", "Construção", "Ferragens"],
        "subcategoria": ["Cimento", "Areia", "Alvenaria", "Pregos"]
    })
    produtos.to_parquet(RAW_DATA_DIR / "produtos.parquet", index=False)
    print("✔ produtos.parquet gerado.")


if __name__ == "__main__":
    gerar_vendas()
    gerar_lojas()
    gerar_produtos()
    print("\n✅ Todos os arquivos .parquet foram gerados com sucesso!")
