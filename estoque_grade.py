# estoque_grade.py
"""
Dump consolidado por grade - estoque atual
Base: KARDEX agrupado por COD_GRADE
Mostra: 1 linha por grade com saldo calculado (entradas - saídas)

TRATAMENTO ESPECIAL:
- Produtos sem COD_GRADE recebem identificador único: {CODIGO_PRODUTO}_UNICO
- Isso garante que cada produto seja contabilizado separadamente
"""
import os
import sys
import pandas as pd
import firebirdsql
import config
from datetime import datetime

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 2000)

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

CHARSETS = ["UTF8", "WIN1252", "ISO8859_1", "DOS850"]

def conectar(charset):
    return firebirdsql.connect(
        host=config.DB_HOST,
        port=int(str(config.DB_PORT or "3369")),
        database=config.DB_PATH,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        charset=charset,
    )

def ler_tabela(sql, nome_tabela):
    print(f"\n📊 Lendo {nome_tabela}...", end=" ", flush=True)
    
    for cs in CHARSETS:
        try:
            conn = conectar(cs)
            df = pd.read_sql(sql, conn)
            conn.close()
            print(f"✅ {len(df):,} linhas")
            return df
        except KeyboardInterrupt:
            print("\n⚠️ Interrompido")
            sys.exit(1)
        except:
            continue
    
    raise Exception(f"Falhou ao ler {nome_tabela}")

def main():
    inicio = datetime.now()
    
    print("=" * 80)
    print("📦 ESTOQUE CONSOLIDADO POR GRADE")
    print("=" * 80)
    
    # ============================================================================
    # 1. LER KARDEX
    # ============================================================================
    df_kardex = ler_tabela("""
        SELECT 
            LOJA, CODIGO_PRODUTO, COD_GRADE, DESCRICAO,
            QTDE_ENTRADA, QTDE_SAIDA, TIPO,
            COD_GRADE_COR, COD_GRADE_TAMANHO,
            NOME_USUARIO, DATA_MOVIMENTO, HORA_MOVIMENTO,
            HISTORICO
        FROM KARDEX
    """, "KARDEX")
    
    # Filtrar linhas com "BALAN" no HISTORICO (pega BALANÇO, BALANCO, etc)
    linhas_antes_balanco = len(df_kardex)
    df_kardex = df_kardex[
        ~df_kardex['HISTORICO'].astype(str).str.upper().str.contains('BALAN', na=False)
    ]
    linhas_removidas_balanco = linhas_antes_balanco - len(df_kardex)
    if linhas_removidas_balanco > 0:
        print(f"   🔍 Removidas {linhas_removidas_balanco:,} linhas com BALANÇO no histórico")
    
    # Remover coluna HISTORICO (não é necessária no resultado final)
    df_kardex = df_kardex.drop(columns=['HISTORICO'], errors='ignore')
    
    # Filtrar apenas movimentações reais
    linhas_antes = len(df_kardex)
    df_kardex = df_kardex[(df_kardex['QTDE_ENTRADA'] > 0) | (df_kardex['QTDE_SAIDA'] > 0)]
    print(f"   🔍 Removidas {linhas_antes - len(df_kardex):,} linhas vazias")
    
    # Tratar COD_GRADE vazio - preencher com identificador único por produto
    grades_vazias_antes = df_kardex['COD_GRADE'].isna().sum() + (df_kardex['COD_GRADE'] == '').sum()
    
    df_kardex['COD_GRADE'] = df_kardex.apply(
        lambda row: row['COD_GRADE'] if (row['COD_GRADE'] and str(row['COD_GRADE']).strip() != '') 
        else f"{row['CODIGO_PRODUTO']}_UNICO",
        axis=1
    )
    
    if grades_vazias_antes > 0:
        print(f"   🔧 Corrigidas {grades_vazias_antes:,} grades vazias (agora únicas por produto)")
    
    # ============================================================================
    # 2. LER PRODUTOS
    # ============================================================================
    try:
        df_produtos = ler_tabela("""
            SELECT 
                REFERENCIA, PRECO_CUST, PRECO_VEND, UNIDADE,
                MARCA, SEGMENTO, GRUPO, SUB_GRUPO
            FROM PRODUTOS
        """, "PRODUTOS")
    except:
        df_produtos = None
        print("   ⚠️ PRODUTOS não disponível")
    
    # ============================================================================
    # 3. LER TABELAS AUXILIARES
    # ============================================================================
    try:
        df_grupos = ler_tabela("SELECT CODIGO, NOME_GRUPO FROM GRUPOS", "GRUPOS")
    except:
        df_grupos = None
    
    try:
        df_marcas = ler_tabela("SELECT CODIGO, NOME_MARCA FROM MARCAS", "MARCAS")
    except:
        df_marcas = None
    
    try:
        df_subgrupo = ler_tabela("SELECT CODIGO, DESCRICAO FROM PRODUTOS_SUB_GRUPO", "PRODUTOS_SUB_GRUPO")
    except:
        df_subgrupo = None
    
    # ============================================================================
    # 4. CONSOLIDAR POR GRADE
    # ============================================================================
    print(f"\n🔗 Consolidando por grade...")
    
    # Converter CODIGO_PRODUTO para string
    df_kardex['CODIGO_PRODUTO'] = df_kardex['CODIGO_PRODUTO'].astype(str)
    
    # Agrupar por COD_GRADE e consolidar
    df_consolidado = df_kardex.groupby('COD_GRADE').agg({
        'LOJA': 'first',
        'CODIGO_PRODUTO': 'first',
        'DESCRICAO': 'first',
        'QTDE_ENTRADA': 'sum',
        'QTDE_SAIDA': 'sum',
        'COD_GRADE_COR': 'first',
        'COD_GRADE_TAMANHO': 'first',
        'NOME_USUARIO': 'last',  # Último usuário que movimentou
        'DATA_MOVIMENTO': 'max',  # Data mais recente
        'HORA_MOVIMENTO': 'last'  # Hora da última movimentação
    }).reset_index()
    
    # Calcular saldo
    df_consolidado['SALDO_GRADE'] = df_consolidado['QTDE_ENTRADA'] - df_consolidado['QTDE_SAIDA']
    
    # Tratar saldos negativos como zero
    df_consolidado['SALDO_GRADE'] = df_consolidado['SALDO_GRADE'].clip(lower=0)
    
    # Filtrar: remover produtos _UNICO com saldo zero
    linhas_antes_filtro = len(df_consolidado)
    df_consolidado = df_consolidado[
        ~((df_consolidado['COD_GRADE'].str.contains('_UNICO', na=False)) & 
          (df_consolidado['SALDO_GRADE'] == 0))
    ]
    linhas_removidas_unico = linhas_antes_filtro - len(df_consolidado)
    if linhas_removidas_unico > 0:
        print(f"   🔍 Removidos {linhas_removidas_unico:,} produtos sem grade e sem saldo")
    
    # Remover colunas de entrada/saída (já estão no saldo)
    df_consolidado = df_consolidado.drop(columns=['QTDE_ENTRADA', 'QTDE_SAIDA'])
    
    print(f"   ✅ {len(df_kardex):,} movimentações → {len(df_consolidado):,} grades únicas")
    
    # ============================================================================
    # 5. ENRIQUECER COM DADOS DE PRODUTO
    # ============================================================================
    if df_produtos is not None:
        # Preparar PRODUTOS com joins
        if df_grupos is not None:
            df_grupos['CODIGO'] = df_grupos['CODIGO'].astype(str)
            df_produtos['GRUPO'] = df_produtos['GRUPO'].astype(str)
            df_produtos = df_produtos.merge(df_grupos, left_on='GRUPO', right_on='CODIGO', how='left')
            df_produtos = df_produtos.drop(columns=['CODIGO'], errors='ignore')
        
        if df_marcas is not None:
            df_marcas['CODIGO'] = df_marcas['CODIGO'].astype(str)
            df_produtos['MARCA'] = df_produtos['MARCA'].astype(str)
            df_produtos = df_produtos.merge(df_marcas, left_on='MARCA', right_on='CODIGO', how='left')
            df_produtos = df_produtos.drop(columns=['CODIGO'], errors='ignore')
        
        if df_subgrupo is not None:
            df_subgrupo['CODIGO'] = df_subgrupo['CODIGO'].astype(str)
            df_produtos['SUB_GRUPO'] = df_produtos['SUB_GRUPO'].astype(str)
            df_produtos = df_produtos.merge(df_subgrupo, left_on='SUB_GRUPO', right_on='CODIGO', how='left', suffixes=('', '_SG'))
            df_produtos = df_produtos.drop(columns=['CODIGO'], errors='ignore')
            if 'DESCRICAO' in df_produtos.columns:
                df_produtos = df_produtos.rename(columns={'DESCRICAO': 'SUBGRUPO_DESC'})
        
        # JOIN com consolidado
        df_produtos['REFERENCIA'] = df_produtos['REFERENCIA'].astype(str)
        df_consolidado = df_consolidado.merge(
            df_produtos,
            left_on='CODIGO_PRODUTO',
            right_on='REFERENCIA',
            how='left'
        )
        print(f"   ✅ Dados de produto enriquecidos")
    
    # Calcular valores monetários (sempre, mesmo sem dados de produtos)
    if 'PRECO_CUST' in df_consolidado.columns:
        df_consolidado['CUSTO_GRADE'] = df_consolidado['SALDO_GRADE'] * df_consolidado['PRECO_CUST'].fillna(0)
    else:
        df_consolidado['CUSTO_GRADE'] = 0
    
    if 'PRECO_VEND' in df_consolidado.columns:
        df_consolidado['VENDA_GRADE'] = df_consolidado['SALDO_GRADE'] * df_consolidado['PRECO_VEND'].fillna(0)
    else:
        df_consolidado['VENDA_GRADE'] = 0
    
    # ============================================================================
    # 6. ORGANIZAR COLUNAS FINAIS
    # ============================================================================
    colunas_finais = {
        'LOJA': 'LOJA',
        'CODIGO_PRODUTO': 'CODIGO_PRODUTO',
        'COD_GRADE': 'COD_GRADE',
        'DESCRICAO': 'DESCRICAO',
        'UNIDADE': 'UNIDADE',
        'NOME_GRUPO': 'GRUPO',
        'SUBGRUPO_DESC': 'SUBGRUPO',
        'NOME_MARCA': 'MARCA',
        'PRECO_CUST': 'PRECO_CUST',
        'PRECO_VEND': 'PRECO_VEND',
        'SALDO_GRADE': 'SALDO_GRADE',
        'CUSTO_GRADE': 'Custo($)_Grade',
        'VENDA_GRADE': 'Venda($)_Grade',
        'COD_GRADE_COR': 'COD_GRADE_COR',
        'COD_GRADE_TAMANHO': 'COD_GRADE_TAMANHO',
        'NOME_USUARIO': 'ULTIMA_MOVIMENTACAO_USUARIO',
        'DATA_MOVIMENTO': 'ULTIMA_MOVIMENTACAO_DATA',
        'HORA_MOVIMENTO': 'ULTIMA_MOVIMENTACAO_HORA'
    }
    
    colunas_existentes = [col for col in colunas_finais.keys() if col in df_consolidado.columns]
    df_final = df_consolidado[colunas_existentes].rename(columns=colunas_finais)
    
    # ============================================================================
    # 7. ESTATÍSTICAS
    # ============================================================================
    print(f"\n📊 Estatísticas:")
    print(f"   • Grades únicas: {len(df_final):,}")
    
    # Contar grades originais vs geradas
    grades_unicas = df_final[~df_final['COD_GRADE'].str.contains('_UNICO', na=False)]
    grades_geradas = df_final[df_final['COD_GRADE'].str.contains('_UNICO', na=False)]
    
    if len(grades_geradas) > 0:
        print(f"     └─ Com grade definida: {len(grades_unicas):,}")
        print(f"     └─ Sem grade (produtos únicos): {len(grades_geradas):,}")
    
    print(f"   • Grades com estoque: {(df_final['SALDO_GRADE'] > 0).sum():,}")
    print(f"   • Grades com saldo zero: {(df_final['SALDO_GRADE'] == 0).sum():,}")
    print(f"   • Saldo total: {df_final['SALDO_GRADE'].sum():,.0f} unidades")
    print(f"   • Colunas: {len(df_final.columns)}")
    
        # Forçar COD_GRADE_TAMANHO como texto (resolve P, M, G)
    if 'COD_GRADE_TAMANHO' in df_final.columns:
        df_final['COD_GRADE_TAMANHO'] = df_final['COD_GRADE_TAMANHO'].astype(str)
        df_final['COD_GRADE_TAMANHO'] = "'" + df_final['COD_GRADE_TAMANHO'].fillna('')
        
    # ============================================================================
    # 8. SALVAR CSV (para upload ao Sheets)
    # ============================================================================
    csv_path = os.path.join(OUTPUT_DIR, "ESTOQUE_GRADE.csv")
    
    print(f"\n💾 Salvando {csv_path}...", end=" ", flush=True)
    
    try:
        df_final.to_csv(csv_path, index=False, encoding='utf-8')
        
        tamanho_mb = os.path.getsize(csv_path) / (1024 * 1024)
        duracao = (datetime.now() - inicio).total_seconds()
        
        print(f"✅ {tamanho_mb:.2f} MB em {duracao:.1f}s")
        
    except PermissionError:
        print(f"\n❌ Arquivo aberto! Feche e tente novamente.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        sys.exit(1)
    
    print("\n" + "=" * 80)
    print(f"🏁 {len(df_final):,} grades consolidadas")
    print("=" * 80)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrompido")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)