# connect_db.py

import sys
import firebirdsql        # driver 100% Python, não precisa de fbclient.dll
import pandas as pd
import config

def conectar_firebird():
    """
    Conecta ao Firebird via firebirdsql e retorna a conexão DB-API.
    """
    try:
        conn = firebirdsql.connect(
            host     = config.DB_HOST,
            port     = int(config.DB_PORT),
            database = config.DB_PATH,
            user     = config.DB_USER,
            password = config.DB_PASSWORD,
            charset  = config.CHARSET
        )
        print("✅ Conexão via firebirdsql bem-sucedida!")
    except Exception as e:
        print("❌ Erro na conexão firebirdsql:", e)
        sys.exit(1)

    return conn

def listar_tabelas(conn):
    """
    Lista todas as tabelas de usuário no banco.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT TRIM(RDB$RELATION_NAME)
        FROM RDB$RELATIONS
        WHERE RDB$VIEW_BLR IS NULL
          AND (RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG = 0)
    """)
    tabelas = [row[0] for row in cur.fetchall()]
    print(f"\n🔍 {len(tabelas)} tabelas encontradas:")
    for t in tabelas:
        print(" -", t)
    return tabelas

def preview_table(conn, table_name, n=5):
    """
    Exibe as primeiras n linhas de uma tabela usando pandas diretamente
    sobre a conexão DB-API.
    """
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name} ROWS {n}", conn)
        print(f"\n📋 Preview de '{table_name}':")
        print(df)
    except Exception as e:
        print(f"❌ Erro ao ler '{table_name}':", e)

if __name__ == "__main__":
    # 1) Conectar
    conn = conectar_firebird()

    # 2) Listar tabelas
    tabelas = listar_tabelas(conn)

    # 3) Pré-visualizar a primeira tabela, se existir
    if tabelas:
        preview_table(conn, tabelas[0], n=5)
    else:
        print("⚠️ Nenhuma tabela para pré-visualizar.")