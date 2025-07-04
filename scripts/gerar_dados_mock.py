import pandas as pd
import numpy as np
from src.config import paths
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))


# 1. Criar produtos
produtos = [
    {"produto_id": 1, "nome_produto": "Cimento", "categoria": "Construção", "subcategoria": "Argamassa"},
    {"produto_id": 2, "nome_produto": "Tijolo", "categoria": "Construção", "subcategoria": "Alvenaria"},
    {"produto_id": 3, "nome_produto": "Tinta", "categoria": "Acabamento", "subcategoria": "Pintura"},
    {"produto_id": 4, "nome_produto": "Parafuso", "categoria": "Ferramentas", "subcategoria": "Fixação"},
    {"produto_id": 5, "nome_produto": "Porta", "categoria": "Madeira", "subcategoria": "Portas e Janelas"},
]
df_produtos = pd.DataFrame(produtos)

# 2. Criar lojas
lojas = [
    {"loja_id": 1, "nome_loja": "Loja Centro", "cidade": "Maceió", "estado": "AL", "regiao": "Nordeste"},
    {"loja_id": 2, "nome_loja": "Loja Industrial", "cidade": "Aracaju", "estado": "SE", "regiao": "Nordeste"},
    {"loja_id": 3, "nome_loja": "Loja Capital", "cidade": "Recife", "estado": "PE", "regiao": "Nordeste"},
]
df_lojas = pd.DataFrame(lojas)

# 3. Criar vendas mock
np.random.seed(42)
datas = pd.date_range(start="2024-01-01", end="2024-03-31", freq="D")
vendas = []
for _ in range(1000):
    vendas.append({
        "data": np.random.choice(datas),
        "loja_id": np.random.choice([1, 2, 3]),
        "produto_id": np.random.choice([1, 2, 3, 4, 5]),
        "quantidade": np.random.randint(1, 20),
        "valor_unitario": round(np.random.uniform(5.0, 100.0), 2)
    })
df_vendas = pd.DataFrame(vendas)

# ✅ Correção para o tipo de dado compatível com PySpark
df_vendas["data"] = df_vendas["data"].astype("datetime64[ms]")

# 4. Salvar Parquet
paths.DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
df_vendas.to_parquet(paths.DATA_RAW_DIR / "vendas.parquet", index=False)
df_lojas.to_parquet(paths.DATA_RAW_DIR / "lojas.parquet", index=False)
df_produtos.to_parquet(paths.DATA_RAW_DIR / "produtos.parquet", index=False)

print("✅ Dados mock salvos com sucesso!")
