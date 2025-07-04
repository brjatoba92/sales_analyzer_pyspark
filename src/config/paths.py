from pathlib import Path

# Caminho da raiz do projeto
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Diretórios de dados
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Criação das pastas, se não existirem
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
