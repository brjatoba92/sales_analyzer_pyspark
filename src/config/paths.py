from pathlib import Path

# Caminho da raiz do projeto
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Diretórios de dados
DATA_DIR = BASE_DIR / "data"
DATA_RAW_DIR = DATA_DIR / "raw"
DATA_PROCESSED_DIR = DATA_DIR / "processed"

# Criação das pastas
DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
