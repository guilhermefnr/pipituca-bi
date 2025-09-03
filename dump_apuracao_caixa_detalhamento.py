# dump_apuracao_caixa_detalhamento.py
import os
import pandas as pd
import firebirdsql
import config

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1800)

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ordem de tentativas de charset
CHARSETS = []
if getattr(config, "CHARSET", None):
    CHARSETS.append(config.CHARSET)
for cs in ["UTF8", "WIN1252", "ISO8859_1", "DOS850"]:
    if cs not in CHARSETS:
        CHARSETS.append(cs)

def connect_with(cs: str):
    """Abre conexão Firebird usando um charset específico."""
    return firebirdsql.connect(
        host=config.DB_HOST,
        port=int(str(config.DB_PORT or "3369")),
        database=config.DB_PATH,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        charset=cs,
    )

def exec_sql(sql: str) -> pd.DataFrame:
    """Executa SQL tentando múltiplos charsets até funcionar."""
    last = None
    for cs in CHARSETS:
        try:
            with connect_with(cs) as conn:
                print(f"→ Lendo com charset={cs} ...")
                return pd.read_sql(sql, conn)
        except Exception as e:
            last = e
            print(f"⚠️ Falhou com charset={cs}: {e}")
    raise last  # type: ignore

if __name__ == "__main__":
    # 1) Contagem (só para feedback)
    try:
        cnt = exec_sql("SELECT COUNT(*) AS CNT FROM APURACAO_CAIXA_DETALHAMENTO")
        total = int(cnt.iloc[0]["CNT"])
        print(f"📦 APURACAO_CAIXA_DETALHAMENTO: {total} linhas")
    except Exception as e:
        print(f"⚠️ Não foi possível contar linhas (seguindo mesmo assim): {e}")
        total = None

    # 2) Dump completo
    sql_full = "SELECT * FROM APURACAO_CAIXA_DETALHAMENTO"
    try:
        df = exec_sql(sql_full)
    except Exception as e:
        print(f"❌ Erro ao ler APURACAO_CAIXA_DETALHAMENTO: {e}")
        raise

    # 3) Salvar arquivos
    csv_path = os.path.join(OUTPUT_DIR, "APURACAO_CAIXA_DETALHAMENTO_FULL.csv")
    xlsx_sample_path = os.path.join(OUTPUT_DIR, "APURACAO_CAIXA_DETALHAMENTO_sample.xlsx")

    # CSV completo
    try:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"✅ CSV completo -> {csv_path}")
    except PermissionError:
        print(f"❌ Permissão negada ao salvar {csv_path}. Feche o arquivo se estiver aberto e rode novamente.")
    except Exception as e:
        print(f"❌ Erro ao salvar CSV: {e}")

    # Amostra em Excel (primeiras 300 linhas) para inspeção rápida
    try:
        sample = df.head(300)
        with pd.ExcelWriter(xlsx_sample_path, engine="openpyxl") as w:
            sample.to_excel(w, index=False, sheet_name="sample")
        print(f"🧾 Amostra (300 linhas) -> {xlsx_sample_path}")
    except Exception as e:
        print(f"⚠️ Não foi possível criar o sample em Excel: {e}")

    print("🏁 Concluído.")