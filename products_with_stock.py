# products_with_stock.py
# Produtos + quantidade calculada + Totais e STATUS/NIVEL (no final)

from connect_db import conectar_firebird
import firebirdsql, pandas as pd, os, config

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1200)

def try_read_df(sql: str) -> pd.DataFrame:
    last_err = None
    for cs in [getattr(config, "CHARSET", "UTF8"), "WIN1252", "ISO8859_1", "DOS850"]:
        try:
            conn = firebirdsql.connect(
                host=config.DB_HOST, port=int(config.DB_PORT),
                database=config.DB_PATH, user=config.DB_USER,
                password=config.DB_PASSWORD, charset=cs,
            )
            return pd.read_sql(sql, conn)
        except Exception as e:
            last_err = e
    raise last_err  # type: ignore

if __name__ == '__main__':

    sql = """
    WITH P AS (
        SELECT p.*, TRIM(UPPER(p.NOME)) AS NUP
        FROM PRODUTOS p
    ),
    AUG AS (
        SELECT
            P.*,
            CASE
              WHEN CHAR_LENGTH(P.NUP) >= 4
                   THEN SUBSTRING(P.NUP FROM CHAR_LENGTH(P.NUP) - 3 FOR 4)
              ELSE NULL
            END AS SUF4,
            -- quantidade calculada (proteção p/ custo = 0)
            CASE
              WHEN P.PRECO_CUST IS NULL OR P.PRECO_CUST = 0 THEN NULL
              ELSE CAST(P.VR_ESTOQUE_TOTAL AS DOUBLE PRECISION) /
                   CAST(P.PRECO_CUST      AS DOUBLE PRECISION)
            END AS QTY_CALC
        FROM P
    )
    SELECT
        A.CODIGO                                                        AS CODIGO,
        A.REFERENCIA                                                    AS REFERENCIA,
        A.NOME                                                          AS NOME,
        A.UNIDADE                                                       AS UNIDADE,
        A.QTY_CALC                                                      AS QUANTIDADE_PRODUTO,
        A.PRECO_CUST                                                    AS PRECO_CUSTO,
        A.PRECO_VEND                                                    AS Preco_Varejo,
        A.VR_ESTOQUE_TOTAL                                              AS "Tot. Custo",
        CASE
          WHEN A.QTY_CALC IS NULL OR A.PRECO_VEND IS NULL THEN NULL
          ELSE CAST(A.PRECO_VEND AS DOUBLE PRECISION) * A.QTY_CALC
        END                                                             AS "Tot. Varejo",
        A.DATA_CADASTRO                                                 AS DATA_CADASTRO,
        -- últimas colunas: STATUS e NIVEL
        CASE
          WHEN A.NUP LIKE '% ESTOQUE' OR A.NUP = 'ESTOQUE' THEN 'ESTOQUE'
          WHEN A.SUF4 SIMILAR TO '[0-9X]{4}' THEN 'COMPRA'
          ELSE NULL
        END                                                             AS STATUS,
        CASE
          WHEN A.NUP LIKE '% ESTOQUE' OR A.NUP = 'ESTOQUE' THEN NULL
          WHEN A.SUF4 SIMILAR TO '[0-9X]{4}' THEN 'PEDIDO ' || A.SUF4
          ELSE NULL
        END                                                             AS NIVEL
    FROM AUG A
    ORDER BY A.CODIGO
    """

    df = try_read_df(sql)

    # ajustes visuais (opcional)
    if 'QUANTIDADE_PRODUTO' in df.columns:
        df['QUANTIDADE_PRODUTO'] = pd.to_numeric(df['QUANTIDADE_PRODUTO'], errors='coerce').round(3)
    for col in ['Preco_Varejo', 'Tot. Custo', 'Tot. Varejo']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    print(f"📊 Linhas: {len(df)} | Colunas: {len(df.columns)}")

    outdir = "output"
    os.makedirs(outdir, exist_ok=True)
    df.to_csv(os.path.join(outdir, "produtos_with_stock_totais.csv"), index=False, encoding="utf-8-sig")
    df.to_excel(os.path.join(outdir, "produtos_with_stock_totais.xlsx"), index=False, engine="openpyxl")
    print("✅ Arquivos gerados em /output (STATUS e NIVEL no final).")