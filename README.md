# Sales Analyzer with PySpark

## Architecture:

analise_vendas_pyspark/
├── README.md
├── requirements.txt
├── pyproject.toml                  # (opcional, se quiser usar poetry)
├── .gitignore
├── data/
│   ├── raw/
│   │   ├── vendas.parquet
│   │   ├── lojas.parquet
│   │   └── produtos.parquet
│   └── processed/
├── notebooks/
│   └── exploracao_inicial.ipynb
├── scripts/
│   └── gerar_dados_mock.py       # <- Aqui vai o script para criar os .parquet
├── src/
│   ├── __init__.py
│   ├── config/
│   │   └── paths.py                # Caminhos dos arquivos
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── load_data.py           # Leitura dos dados parquet
│   │   ├── transform.py           # Limpeza, joins, agregações
│   └── analysis/
│       ├── __init__.py
│       ├── vendas_por_loja.py     # Funções para análise e ranking
│       └── filtros.py             # Filtros por data, categoria etc.
├── main.py                         # Script principal de execução
└── reports/
    ├── tabelas/
    └── graficos/
