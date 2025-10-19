# dump_cc_cotacao_produtos.py
import os
import sys

# Ajusta o path para encontrar o config.py na pasta pai
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import firebirdsql
import config
from datetime import datetime

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1800)

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Ordem de tentativas de charset
CHARSETS = []
if getattr(config, "CHARSET", None):
    CHARSETS.append(config.CHARSET)
for cs in ["UTF8", "WIN1252", "ISO8859_1", "DOS850"]:
    if cs not in CHARSETS:
        CHARSETS.append(cs)

# Limite de linhas para teste (None = todas)
LIMIT_ROWS = None

def connect_with(cs: str):
    """Abre conexão Firebird usando um charset específico."""
    conn = firebirdsql.connect(
        host=config.DB_HOST,
        port=int(str(config.DB_PORT or "3369")),
        database=config.DB_PATH,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        charset=cs,
    )
    return conn

def exec_sql(sql: str) -> tuple[pd.DataFrame, str]:
    """Executa SQL tentando múltiplos charsets até funcionar."""
    last_error = None
    
    for idx, cs in enumerate(CHARSETS, 1):
        conn = None
        try:
            print(f"→ Tentativa {idx}/{len(CHARSETS)}: charset={cs}...", end=" ", flush=True)
            conn = connect_with(cs)
            df = pd.read_sql(sql, conn)
            print(f"✅ Sucesso! ({len(df)} linhas)")
            return df, cs
        except KeyboardInterrupt:
            print("\n⚠️ Interrompido pelo usuário (Ctrl+C)")
            sys.exit(1)
        except Exception as e:
            error_msg = str(e)
            if "op_code" in error_msg:
                error_msg = f"Erro de comunicação Firebird (op_code)"
            elif "transliteration" in error_msg.lower():
                error_msg = "Charset incompatível"
            print(f"❌ {error_msg}")
            last_error = e
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass
    raise Exception(f"Falhou com todos os charsets. Último erro: {last_error}")

def main():
    inicio = datetime.now()
    print("=" * 60)
    print("📦 Iniciando dump de CC_COTACAO_PRODUTOS")
    print("=" * 60)
    
    sql_full = "SELECT * FROM CC_COTACAO_PRODUTOS"
    if LIMIT_ROWS:
        sql_full = f"SELECT FIRST {LIMIT_ROWS} * FROM CC_COTACAO_PRODUTOS"
        print(f"⚠️ MODO TESTE: Limitado a {LIMIT_ROWS} linhas")
    
    try:
        print("\n📊 Lendo dados da tabela CC_COTACAO_PRODUTOS...")
        df, charset_usado = exec_sql(sql_full)
        
        print(f"\n✅ Dados carregados com sucesso!")
        print(f"   • Charset: {charset_usado}")
        print(f"   • Linhas: {len(df):,}")
        print(f"   • Colunas: {len(df.columns)}")
        
        if len(df.columns) > 0:
            preview_cols = list(df.columns[:8])
            print(f"   • Primeiras colunas: {', '.join(preview_cols)}")
        
        if len(df) > 0 and len(df) <= 50:
            print(f"\n📋 Preview dos dados:")
            print(df.head(10).to_string())
        elif len(df) > 0:
            print(f"\n📋 Preview dos dados (5 primeiras linhas):")
            print(df.head(5).to_string())
            
    except KeyboardInterrupt:
        print("\n⚠️ Operação cancelada pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERRO FATAL ao ler CC_COTACAO_PRODUTOS: {e}")
        sys.exit(1)
    
    xlsx_path = os.path.join(OUTPUT_DIR, "CC_COTACAO_PRODUTOS_FULL.xlsx")
    
    print(f"\n💾 Salvando em Excel...")
    print(f"   Arquivo: {xlsx_path}")
    
    try:
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="CC_COTACAO_PRODUTOS")
        
        tamanho_mb = os.path.getsize(xlsx_path) / (1024 * 1024)
        print(f"\n✅ Arquivo salvo com sucesso!")
        print(f"   • Tamanho: {tamanho_mb:.2f} MB")
        
    except PermissionError:
        print(f"\n❌ ERRO: Permissão negada ao salvar {xlsx_path}")
        print(f"   → O arquivo está aberto no Excel? Feche-o e tente novamente.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERRO ao salvar Excel: {e}")
        sys.exit(1)
    
    fim = datetime.now()
    duracao = (fim - inicio).total_seconds()
    
    print("\n" + "=" * 60)
    print(f"🏁 Concluído em {duracao:.1f} segundos")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Processo interrompido pelo usuário")
        sys.exit(1)
