# Sales Analyzer with PySpark

## Objective
Create basic aggregated sales reports by store.

## Features
- Load sales data from multiple stores
- Simple aggregations: total sales, item count, average ticket size
- Store ranking by revenue
- Filters by date range and product category

## Required Datasets
1. `sales.parquet`:  
   Columns: `date`, `store_id`, `product_id`, `quantity`, `unit_price`

2. `stores.parquet`:  
   Columns: `store_id`, `store_name`, `city`, `state`, `region`

3. `products.parquet`:  
   Columns: `product_id`, `product_name`, `category`, `subcategory`

## Key PySpark Concepts
- Basic DataFrame API
- Aggregations with `groupBy()` and `agg()`
- Simple joins
- Date functions and sorting

## Architecture:
```
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
```