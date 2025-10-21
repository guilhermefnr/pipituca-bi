"""
Relatório de Saídas por Grade - Vendas e Retiradas
Base: KARDEX filtrado por tipo de movimento (VENDA e RETIRADA)
Mostra: 1 linha por grade/data/usuário com total de saídas

IMPORTANTE:
- Filtra movimentações do HISTORICO que contenham "VENDA" ou "RETIRADA"
- Agrupa por: COD_GRADE + DATA_MOVIMENTO + NOME_USUARIO
- Calcula: QTD_SAIDA (soma de todas as saídas do grupo)
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
    print("🛒 RELATÓRIO DE SAÍDAS POR GRADE (VENDAS E RETIRADAS)")
    print("=" * 80)
    
    # ============================================================================
    # 1. LER KARDEX COMPLETO
    # ============================================================================
    df_kardex = ler_tabela("""
        SELECT 
            LOJA, CODIGO_PRODUTO, COD_GRADE, DESCRICAO,
            QTDE_SAIDA, TIPO,
            COD_GRADE_COR, COD_GRADE_TAMANHO,
            HISTORICO, NOME_USUARIO,
            DATA_MOVIMENTO, HORA_MOVIMENTO
        FROM KARDEX
    """, "KARDEX")
    
    # Tratar COD_GRADE vazio - preencher com identificador único por produto
    grades_vazias_antes = df_kardex['COD_GRADE'].isna().sum() + (df_kardex['COD_GRADE'] == '').sum()
    
    df_kardex['COD_GRADE'] = df_kardex.apply(
        lambda row: row['COD_GRADE'] if (row['COD_GRADE'] and str(row['COD_GRADE']).strip() != '') 
        else f"{row['CODIGO_PRODUTO']}_UNICO",
        axis=1
    )
    
    if grades_vazias_antes > 0:
        print(f"   🔧 Corrigidas {grades_vazias_antes:,} grades vazias")
    
    # ============================================================================
    # 2. FILTRAR SAÍDAS (VENDAS E RETIRADAS)
    # ============================================================================
    print(f"\n🔍 Filtrando SAÍDAS (VENDAS e RETIRADAS) do kardex...")
    
    # Filtrar registros que são VENDAS ou RETIRADAS
    df_saidas = df_kardex[
        df_kardex['HISTORICO'].astype(str).str.upper().str.contains('VENDA|RETIRADA', na=False)
    ].copy()
    
    print(f"   ✅ {len(df_saidas):,} movimentações de saída encontradas")
    
    # ============================================================================
    # 3. AGRUPAR POR GRADE + DATA + USUARIO
    # ============================================================================
    print(f"\n📦 Agrupando por COD_GRADE + DATA_MOVIMENTO + NOME_USUARIO...")
    
    # Consolidar saídas por grade/data/usuário
    df_consolidado = df_saidas.groupby(['COD_GRADE', 'DATA_MOVIMENTO', 'NOME_USUARIO']).agg({
        'LOJA': 'first',
        'CODIGO_PRODUTO': 'first',
        'DESCRICAO': 'first',
        'QTDE_SAIDA': 'sum',  # SOMA todas as saídas (VENDA + RETIRADA)
        'COD_GRADE_COR': 'first',
        'COD_GRADE_TAMANHO': 'first',
        'HORA_MOVIMENTO': 'first'  # Primeira hora do dia
    }).reset_index()
    
    df_consolidado = df_consolidado.rename(columns={'QTDE_SAIDA': 'QTD_SAIDA'})
    
    print(f"   ✅ {len(df_consolidado):,} linhas (grade/data/usuário)")
    print(f"   📊 Total de saídas: {df_consolidado['QTD_SAIDA'].sum():,.0f} unidades")
    
    # ============================================================================
    # 4. LER PRODUTOS E ENRIQUECER
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
    
    # Ler tabelas auxiliares
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
    
    # Enriquecer com dados de produto
    if df_produtos is not None:
        print(f"\n🔗 Enriquecendo com dados de produto...")
        
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
        df_consolidado['CODIGO_PRODUTO'] = df_consolidado['CODIGO_PRODUTO'].astype(str)
        df_produtos['REFERENCIA'] = df_produtos['REFERENCIA'].astype(str)
        
        df_consolidado = df_consolidado.merge(
            df_produtos,
            left_on='CODIGO_PRODUTO',
            right_on='REFERENCIA',
            how='left'
        )
        print(f"   ✅ Dados de produto enriquecidos")
    
    # ============================================================================
    # 5. CALCULAR MÉTRICAS
    # ============================================================================
    print(f"\n📊 Calculando métricas...")
    
    # Calcular MKP (Markup = Preço Venda / Preço Custo)
    if 'PRECO_VEND' in df_consolidado.columns and 'PRECO_CUST' in df_consolidado.columns:
        df_consolidado['MKP'] = df_consolidado.apply(
            lambda row: row['PRECO_VEND'] / row['PRECO_CUST'] 
                        if row['PRECO_CUST'] > 0 
                        else 0,
            axis=1
        ).round(2)
        print(f"   ✅ MKP médio: {df_consolidado['MKP'].mean():.2f}")
    else:
        df_consolidado['MKP'] = 0
    
    # Calcular FATURAMENTO = QTD_SAIDA × PRECO_VENDA
    if 'PRECO_VEND' in df_consolidado.columns:
        df_consolidado['FATURAMENTO'] = (df_consolidado['QTD_SAIDA'] * df_consolidado['PRECO_VEND'].fillna(0)).round(2)
        print(f"   ✅ Faturamento total: R$ {df_consolidado['FATURAMENTO'].sum():,.2f}")
    else:
        df_consolidado['FATURAMENTO'] = 0
    
    # Calcular CUSTO_TOTAL = QTD_SAIDA × PRECO_CUSTO
    if 'PRECO_CUST' in df_consolidado.columns:
        df_consolidado['CUSTO_TOTAL'] = (df_consolidado['QTD_SAIDA'] * df_consolidado['PRECO_CUST'].fillna(0)).round(2)
        print(f"   ✅ Custo total: R$ {df_consolidado['CUSTO_TOTAL'].sum():,.2f}")
    else:
        df_consolidado['CUSTO_TOTAL'] = 0
    
    # Calcular LUCRO_BRUTO = FATURAMENTO - CUSTO_TOTAL
    df_consolidado['LUCRO_BRUTO'] = (df_consolidado['FATURAMENTO'] - df_consolidado['CUSTO_TOTAL']).round(2)
    print(f"   ✅ Lucro bruto total: R$ {df_consolidado['LUCRO_BRUTO'].sum():,.2f}")
    
    # Calcular MARGEM_BRUTA = (FATURAMENTO / CUSTO_TOTAL)
    df_consolidado['MARGEM_BRUTA'] = df_consolidado.apply(
        lambda row: ((row['FATURAMENTO'] / row['CUSTO_TOTAL'])) 
                    if row['CUSTO_TOTAL'] > 0 
                    else 0,
        axis=1
    ).round(2)
    print(f"   ✅ Margem bruta média: {df_consolidado['MARGEM_BRUTA'].mean():.2f}")
    
    # ============================================================================
    # 6. ORGANIZAR COLUNAS FINAIS
    # ============================================================================
    colunas_finais = {
        'LOJA': 'LOJA',
        'DATA_MOVIMENTO': 'DATA_MOVIMENTO',
        'HORA_MOVIMENTO': 'HORA_MOVIMENTO',
        'NOME_USUARIO': 'NOME_USUARIO',
        'CODIGO_PRODUTO': 'CODIGO_PRODUTO',
        'COD_GRADE': 'COD_GRADE',
        'DESCRICAO': 'DESCRICAO',
        'COD_GRADE_COR': 'COR',
        'COD_GRADE_TAMANHO': 'TAMANHO',
        'UNIDADE': 'UNIDADE',
        'NOME_GRUPO': 'GRUPO',
        'SUBGRUPO_DESC': 'SUBGRUPO',
        'NOME_MARCA': 'MARCA',
        'QTD_SAIDA': 'QTD_SAIDA',
        'PRECO_CUST': 'PRECO_CUSTO',
        'PRECO_VEND': 'PRECO_VENDA',
        'MKP': 'MKP',
        'FATURAMENTO': 'FATURAMENTO',
        'CUSTO_TOTAL': 'CUSTO_TOTAL',
        'LUCRO_BRUTO': 'LUCRO_BRUTO',
        'MARGEM_BRUTA': 'MARGEM_BRUTA'
    }
    
    colunas_existentes = [col for col in colunas_finais.keys() if col in df_consolidado.columns]
    df_final = df_consolidado[colunas_existentes].rename(columns=colunas_finais)
    
    # ============================================================================
    # 7. ESTATÍSTICAS
    # ============================================================================
    print(f"\n📊 Estatísticas:")
    print(f"   • Total de linhas: {len(df_final):,}")
    print(f"   • Grades únicas: {df_final['COD_GRADE'].nunique():,}")
    print(f"   • Datas únicas: {df_final['DATA_MOVIMENTO'].nunique():,}")
    print(f"   • Usuários únicos: {df_final['NOME_USUARIO'].nunique():,}")
    print(f"   • Total de saídas: {df_final['QTD_SAIDA'].sum():,.0f} unidades")
    print(f"   • Colunas: {len(df_final.columns)}")
    
    # ============================================================================
    # 8. SALVAR
    # ============================================================================
    xlsx_path = os.path.join(OUTPUT_DIR, "SAIDA_GRADE.xlsx")
    
    print(f"\n💾 Salvando {xlsx_path}...", end=" ", flush=True)
    
    try:
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            df_final.to_excel(writer, index=False, sheet_name="SAIDA_GRADE")
        
        tamanho_mb = os.path.getsize(xlsx_path) / (1024 * 1024)
        duracao = (datetime.now() - inicio).total_seconds()
        
        print(f"✅ {tamanho_mb:.2f} MB em {duracao:.1f}s")
        
    except PermissionError:
        print(f"\n❌ Arquivo aberto! Feche e tente novamente.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        sys.exit(1)
    
    print("\n" + "=" * 80)
    print(f"🏁 {len(df_final):,} linhas de saída processadas")
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