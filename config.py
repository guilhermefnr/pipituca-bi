# config.py
import os
from dotenv import load_dotenv

load_dotenv()

def _getenv(*names, default=None):
    """Retorna o primeiro env não vazio dentre os nomes informados."""
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return v.strip()
    return default

# Aceita FB_* (local), DB_* (legado) e SYNDATA_* (GitHub)
DB_HOST = _getenv("FB_HOST", "DB_HOST", "SYNDATA_HOST")
DB_PORT = _getenv("FB_PORT", "DB_PORT", "SYNDATA_PORT", default="3369")  # default
DB_PATH = _getenv("FB_PATH", "DB_PATH", "SYNDATA_DB_PATH")
DB_USER = _getenv("FB_USER", "DB_USER", "SYNDATA_USER")
DB_PASSWORD = _getenv("FB_PASS", "DB_PASSWORD", "SYNDATA_PASSWORD")
CHARSET = _getenv("FB_CHARSET", "CHARSET", "SYNDATA_CHARSET", default="UTF8")

def testar_config():
    print("=== Variáveis de Configuração ===")
    print(f"DB_HOST:     {DB_HOST}")
    print(f"DB_PORT:     {DB_PORT}")
    print(f"DB_PATH:     {DB_PATH}")
    print(f"DB_USER:     {DB_USER}")
    print(f"DB_PASSWORD: {'****' if DB_PASSWORD else ''}")
    print(f"CHARSET:     {CHARSET}")