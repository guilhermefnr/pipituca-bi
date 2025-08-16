# config.py

from dotenv import load_dotenv
import os

# 1. Carrega as variáveis definidas no arquivo .env
load_dotenv()

# 2. Lê cada variável de ambiente
DB_USER     = os.getenv('FB_USER')
DB_PASSWORD = os.getenv('FB_PASS')
DB_HOST     = os.getenv('FB_HOST')
DB_PORT     = os.getenv('FB_PORT')
DB_PATH     = os.getenv('FB_PATH')
CHARSET     = os.getenv('FB_CHARSET')

# 3. Função simples para testar se tudo carregou corretamente
def testar_config():
    print("=== Variáveis de Configuração ===")
    print(f"DB_USER:     {DB_USER}")
    print(f"DB_PASSWORD: {DB_PASSWORD}")
    print(f"DB_HOST:     {DB_HOST}")
    print(f"DB_PORT:     {DB_PORT}")
    print(f"DB_PATH:     {DB_PATH}")
    print(f"CHARSET:     {CHARSET}")