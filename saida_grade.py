"""
Relatório de Saídas por Grade - Vendas e Retiradas (COM CARGA INCREMENTAL)
Base: KARDEX filtrado por tipo de movimento (VENDA e RETIRADA)
Mostra: 1 linha por grade/data/usuário com total de saídas

MODO INCREMENTAL:
- Mantém arquivo de controle com última execução (.last_run)
- Busca apenas registros desde a última execução
- Faz UPSERT no CSV existente (atualiza ou insere)
- Força carga completa via flag --full ou se não houver histórico
- Cacheia tabelas auxiliares (PRODUTOS, GRUPOS, MARCAS, SUBGRUPO)

IMPORTANTE:
- Filtra movimentações do HISTORICO que contenham "VENDA PDV" ou "RETIRADA" (exceto canceladas)
- Filtra também DEVOLUÇÕES efetivas (para cálculo de custo líquido)
- Agrupa por: COD_GRADE + DATA_MOVIMENTO + NOME_USUARIO + TIPO_MOV
- Calcula: QTD_SAIDA (soma de todas as saídas do grupo)
"""
import os
import sys
import argparse
import pandas as pd
import firebirdsql
import config
from datetime import datetime, timedelta
import json

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 2000)

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Arquivos de controle e saída
LAST_RUN_FILE = os.path.join(OUTPUT_DIR, ".saida_grade_last_run.json")
CSV_PATH = os.path.join(OUTPUT_DIR, "SAIDA_GRADE.csv")

# Arquivos de cache para tabelas auxiliares
CACHE_PRODUTOS = os.path.join(OUTPUT_DIR, ".cache_produtos.json")
CACHE_GRUPOS = os.path.join(OUTPUT_DIR, ".cache_grupos.json")
CACHE_MARCAS = os.path.join(OUTPUT_DIR, ".cache_marcas.json")
CACHE_SUBGRUPO = os.path.join(OUTPUT_DIR, ".cache_subgrupo.json")

# Janela de segurança: buscar dados desde X horas antes da última execução
SAFETY_WINDOW_HOURS = 2

CHARSETS = ["UTF8", "WIN1252", "ISO8859_1", "DOS850"]


def parse_args():
    parser = argparse.ArgumentParser(description='Relatório de Saídas por Grade (Incremental)')
    parser.add_argument('--full', action='store_true', 
                        help='Força carga completa (ignora incremental)')
    parser.add_argument('--days', type=int, default=None,
                        help='Busca apenas os últimos N dias')
    return parser.parse_args()


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


def save_cache(df, cache_path):
    """Salva DataFrame em cache JSON"""
    try:
        df.to_json(cache_path, orient='records', date_format='iso')
    except Exception as e:
        print(f"   ⚠️ Erro ao salvar cache: {e}")


def load_cache(cache_path, nome_tabela):
    """Carrega DataFrame do cache JSON"""
    if os.path.exists(cache_path):
        try:
            df = pd.read_json(cache_path, orient='records')
            print(f"\n📦 Cache {nome_tabela}... ✅ {len(df):,} linhas")
            return df
        except Exception as e:
            print(f"\n⚠️ Erro ao ler cache {nome_tabela}: {e}")
    return None


def get_last_run_info():
    """Lê informações da última execução"""
    if os.path.exists(LAST_RUN_FILE):
        try:
            with open(LAST_RUN_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return None


def save_last_run_info(info):
    """Salva informações da execução atual"""
    with open(LAST_RUN_FILE, 'w') as f:
        json.dump(info, f, indent=2, default=str)


def load_existing_data():
    """Carrega dados existentes do CSV"""
    if os.path.exists(CSV_PATH):
        try:
            df = pd.read_csv(CSV_PATH, dtype={'TAMANHO': str})
            # Remove o prefixo de apóstrofo do TAMANHO para poder fazer merge
            if 'TAMANHO' in df.columns:
                df['TAMANHO'] = df['TAMANHO'].str.lstrip("'")
            print(f"   📂 CSV existente: {len(df):,} linhas")
            return df
        except Exception as e:
            print(f"   ⚠️ Erro ao ler CSV existente: {e}")
    return None


def build_kardex_query(data_corte=None, dias=None):
    """Constrói a query do KARDEX com filtros opcionais"""
    base_query = """
        SELECT 
            LOJA, CODIGO_PRODUTO, COD_GRADE, DESCRICAO,
            QTDE_SAIDA, QTDE_ENTRADA, TIPO,
            COD_GRADE_COR, COD_GRADE_TAMANHO,
            HISTORICO, NOME_USUARIO,
            DATA_MOVIMENTO, HORA_MOVIMENTO,
            NUMERO_PEDIDO
        FROM KARDEX
    """
    
    conditions = []
    
    if data_corte:
        data_str = data_corte.strftime('%Y-%m-%d')
        conditions.append(f"DATA_MOVIMENTO >= '{data_str}'")
    
    if dias:
        data_inicio = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')
        conditions.append(f"DATA_MOVIMENTO >= '{data_inicio}'")
    
    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)
    
    return base_query


def merge_dataframes(df_existing, df_new):
    """
    Faz merge (upsert) dos dados novos com os existentes.
    Chave: COD_GRADE + DATA_MOVIMENTO + NOME_USUARIO + TIPO_MOV
    """
    if df_existing is None or len(df_existing) == 0:
        return df_new
    
    if df_new is None or len(df_new) == 0:
        return df_existing
    
    print(f"\n🔄 Fazendo merge dos dados...")
    print(f"   📂 Existentes: {len(df_existing):,} linhas")
    print(f"   🆕 Novos: {len(df_new):,} linhas")
    
    key_cols = ['COD_GRADE', 'DATA_MOVIMENTO', 'NOME_USUARIO', 'TIPO_MOV']
    
    # Garantir que ambos têm as mesmas colunas
    all_cols = list(set(df_existing.columns) | set(df_new.columns))
    
    for col in all_cols:
        if col not in df_existing.columns:
            df_existing[col] = None
        if col not in df_new.columns:
            df_new[col] = None
    
    # Criar coluna de chave para identificação
    df_existing['_key'] = df_existing[key_cols].astype(str).agg('|'.join, axis=1)
    df_new['_key'] = df_new[key_cols].astype(str).agg('|'.join, axis=1)
    
    # Separar registros existentes que NÃO serão atualizados
    keys_to_update = set(df_new['_key'])
    df_keep = df_existing[~df_existing['_key'].isin(keys_to_update)].copy()
    
    # Contar atualizações vs inserções
    keys_existing = set(df_existing['_key'])
    keys_new = set(df_new['_key'])
    updates = len(keys_existing & keys_new)
    inserts = len(keys_new - keys_existing)
    
    print(f"   📝 Atualizações: {updates:,}")
    print(f"   ➕ Inserções: {inserts:,}")
    
    # Combinar: registros mantidos + novos/atualizados
    df_merged = pd.concat([df_keep, df_new], ignore_index=True)
    
    # Remover coluna auxiliar
    df_merged = df_merged.drop(columns=['_key'], errors='ignore')
    
    print(f"   ✅ Total após merge: {len(df_merged):,} linhas")
    
    return df_merged


def main():
    args = parse_args()
    inicio = datetime.now()
    
    print("=" * 80)
    print("🛒 RELATÓRIO DE SAÍDAS POR GRADE (VENDAS E RETIRADAS)")
    print("=" * 80)
    
    # Determinar modo de execução
    last_run = get_last_run_info()
    is_incremental = False
    data_corte = None
    use_cache = False
    
    if args.full:
        print("\n⚡ MODO: Carga COMPLETA (--full)")
    elif args.days:
        print(f"\n⚡ MODO: Últimos {args.days} dias (--days)")
    elif last_run and os.path.exists(CSV_PATH):
        is_incremental = True
        use_cache = True
        last_run_time = datetime.fromisoformat(last_run['timestamp'])
        data_corte = last_run_time - timedelta(hours=SAFETY_WINDOW_HOURS)
        print(f"\n⚡ MODO: INCREMENTAL")
        print(f"   📅 Última execução: {last_run['timestamp']}")
        print(f"   📅 Buscando desde: {data_corte.isoformat()} (janela de {SAFETY_WINDOW_HOURS}h)")
    else:
        print("\n⚡ MODO: Carga COMPLETA (primeira execução)")
    
    # ============================================================================
    # 1. LER KARDEX (com filtro se incremental)
    # ============================================================================
    query = build_kardex_query(data_corte=data_corte, dias=args.days)
    df_kardex = ler_tabela(query, "KARDEX")
    
    if len(df_kardex) == 0:
        print("\n⚠️ Nenhum registro encontrado no período")
        if is_incremental:
            print("   Mantendo dados existentes...")
            save_last_run_info({
                'timestamp': inicio.isoformat(),
                'mode': 'incremental',
                'records_processed': 0
            })
        return
    
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
    # 2. FILTRAR SAÍDAS (VENDAS PDV + RETIRADAS EFETIVAS) E DEVOLUÇÕES
    # ============================================================================
    print(f"\n🔍 Filtrando SAÍDAS (VENDA PDV + RETIRADA efetivas) e DEVOLUÇÕES...")
    
    # Converter HISTORICO para string e uppercase para comparações
    df_kardex['HISTORICO'] = df_kardex['HISTORICO'].astype(str).str.upper()
    
    # Identificar pedidos que foram cancelados (para excluir suas retiradas/devoluções)
    # Usa 'CANC' para pegar tanto 'CANCELAMENTO' quanto 'CANC. RETIRADA'
    pedidos_cancelados = df_kardex[
        df_kardex['HISTORICO'].str.contains('CANC', na=False)
    ]['NUMERO_PEDIDO'].unique()
    
    if len(pedidos_cancelados) > 0:
        print(f"   ℹ️ Pedidos cancelados identificados: {len(pedidos_cancelados)}")
    
    # --- VENDAS: VENDA PDV + RETIRADA efetivas ---
    df_vendas = df_kardex[
        # Vendas PDV (balcão) - sempre incluir
        (df_kardex['HISTORICO'].str.contains('VENDA PDV', na=False)) |
        # Retiradas (crediário) - apenas se não canceladas
        (
            (df_kardex['HISTORICO'].str.contains('RETIRADA', na=False)) &
            (~df_kardex['HISTORICO'].str.contains('CANC', na=False)) &
            (~df_kardex['NUMERO_PEDIDO'].isin(pedidos_cancelados))
        )
    ].copy()
    df_vendas['TIPO_MOV'] = 'VENDA'
    df_vendas['QTD_MOV'] = df_vendas['QTDE_SAIDA']
    
    print(f"   ✅ {len(df_vendas):,} movimentações de VENDA encontradas")
    
    # --- DEVOLUÇÕES: efetivas (não canceladas) ---
    df_devolucoes = df_kardex[
        (df_kardex['HISTORICO'].str.contains('DEVOLUÇÃO', na=False)) &
        (~df_kardex['HISTORICO'].str.contains('CANC', na=False)) &
        (~df_kardex['NUMERO_PEDIDO'].isin(pedidos_cancelados))
    ].copy()
    df_devolucoes['TIPO_MOV'] = 'DEVOLUCAO'
    df_devolucoes['QTD_MOV'] = df_devolucoes['QTDE_ENTRADA']
    
    print(f"   ✅ {len(df_devolucoes):,} movimentações de DEVOLUÇÃO encontradas")
    
    # --- CONCATENAR VENDAS + DEVOLUÇÕES ---
    df_saidas = pd.concat([df_vendas, df_devolucoes], ignore_index=True)
    
    print(f"   ✅ {len(df_saidas):,} movimentações totais")
    
    if len(df_saidas) == 0:
        print("\n⚠️ Nenhuma saída encontrada no período")
        if is_incremental:
            save_last_run_info({
                'timestamp': inicio.isoformat(),
                'mode': 'incremental',
                'records_processed': 0
            })
        return
    
    # ============================================================================
    # 3. AGRUPAR POR GRADE + DATA + USUARIO + TIPO_MOV
    # ============================================================================
    print(f"\n📦 Agrupando por COD_GRADE + DATA_MOVIMENTO + NOME_USUARIO + TIPO_MOV...")
    
    df_consolidado = df_saidas.groupby(['COD_GRADE', 'DATA_MOVIMENTO', 'NOME_USUARIO', 'TIPO_MOV']).agg({
        'LOJA': 'first',
        'CODIGO_PRODUTO': 'first',
        'DESCRICAO': 'first',
        'QTD_MOV': 'sum',
        'COD_GRADE_COR': 'first',
        'COD_GRADE_TAMANHO': 'first',
        'HORA_MOVIMENTO': 'first'
    }).reset_index()
    
    df_consolidado = df_consolidado.rename(columns={'QTD_MOV': 'QTD_SAIDA'})
    
    print(f"   ✅ {len(df_consolidado):,} linhas (grade/data/usuário/tipo)")
    print(f"   📊 Total de movimentações: {df_consolidado['QTD_SAIDA'].sum():,.0f} unidades")
    
    # ============================================================================
    # 4. LER PRODUTOS E ENRIQUECER (com cache em modo incremental)
    # ============================================================================
    df_produtos = None
    df_grupos = None
    df_marcas = None
    df_subgrupo = None
    
    if use_cache:
        # Tentar carregar do cache
        df_produtos = load_cache(CACHE_PRODUTOS, "PRODUTOS")
        df_grupos = load_cache(CACHE_GRUPOS, "GRUPOS")
        df_marcas = load_cache(CACHE_MARCAS, "MARCAS")
        df_subgrupo = load_cache(CACHE_SUBGRUPO, "SUBGRUPO")
    
    # Se não tem cache ou é carga completa, buscar do banco
    if df_produtos is None:
        try:
            df_produtos = ler_tabela("""
                SELECT 
                    REFERENCIA, PRECO_CUST, PRECO_VEND, UNIDADE,
                    MARCA, SEGMENTO, GRUPO, SUB_GRUPO
                FROM PRODUTOS
            """, "PRODUTOS")
            save_cache(df_produtos, CACHE_PRODUTOS)
        except:
            print("   ⚠️ PRODUTOS não disponível")
    
    if df_grupos is None:
        try:
            df_grupos = ler_tabela("SELECT CODIGO, NOME_GRUPO FROM GRUPOS", "GRUPOS")
            save_cache(df_grupos, CACHE_GRUPOS)
        except:
            pass
    
    if df_marcas is None:
        try:
            df_marcas = ler_tabela("SELECT CODIGO, NOME_MARCA FROM MARCAS", "MARCAS")
            save_cache(df_marcas, CACHE_MARCAS)
        except:
            pass
    
    if df_subgrupo is None:
        try:
            df_subgrupo = ler_tabela("SELECT CODIGO, DESCRICAO FROM PRODUTOS_SUB_GRUPO", "PRODUTOS_SUB_GRUPO")
            save_cache(df_subgrupo, CACHE_SUBGRUPO)
        except:
            pass
    
    if df_produtos is not None:
        print(f"\n🔗 Enriquecendo com dados de produto...")
        
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
    
    if 'PRECO_VEND' in df_consolidado.columns:
        df_consolidado['FATURAMENTO'] = (df_consolidado['QTD_SAIDA'] * df_consolidado['PRECO_VEND'].fillna(0)).round(2)
        print(f"   ✅ Faturamento total: R$ {df_consolidado['FATURAMENTO'].sum():,.2f}")
    else:
        df_consolidado['FATURAMENTO'] = 0
    
    if 'PRECO_CUST' in df_consolidado.columns:
        df_consolidado['CUSTO_TOTAL'] = (df_consolidado['QTD_SAIDA'] * df_consolidado['PRECO_CUST'].fillna(0)).round(2)
        print(f"   ✅ Custo total: R$ {df_consolidado['CUSTO_TOTAL'].sum():,.2f}")
    else:
        df_consolidado['CUSTO_TOTAL'] = 0
    
    df_consolidado['LUCRO_BRUTO'] = (df_consolidado['FATURAMENTO'] - df_consolidado['CUSTO_TOTAL']).round(2)
    print(f"   ✅ Lucro bruto total: R$ {df_consolidado['LUCRO_BRUTO'].sum():,.2f}")
    
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
        'MARGEM_BRUTA': 'MARGEM_BRUTA',
        'TIPO_MOV': 'TIPO_MOV'
    }
    
    colunas_existentes = [col for col in colunas_finais.keys() if col in df_consolidado.columns]
    df_new = df_consolidado[colunas_existentes].rename(columns=colunas_finais)
    
    # ============================================================================
    # 7. MERGE COM DADOS EXISTENTES (se incremental)
    # ============================================================================
    if is_incremental:
        df_existing = load_existing_data()
        df_final = merge_dataframes(df_existing, df_new)
    else:
        df_final = df_new
    
    # ============================================================================
    # 8. ESTATÍSTICAS
    # ============================================================================
    print(f"\n📊 Estatísticas:")
    print(f"   • Total de linhas: {len(df_final):,}")
    print(f"   • Grades únicas: {df_final['COD_GRADE'].nunique():,}")
    print(f"   • Datas únicas: {df_final['DATA_MOVIMENTO'].nunique():,}")
    print(f"   • Usuários únicos: {df_final['NOME_USUARIO'].nunique():,}")
    print(f"   • Total de movimentações: {df_final['QTD_SAIDA'].sum():,.0f} unidades")
    print(f"   • Colunas: {len(df_final.columns)}")
    if 'TIPO_MOV' in df_final.columns:
        print(f"   • Vendas: {len(df_final[df_final['TIPO_MOV'] == 'VENDA']):,} linhas")
        print(f"   • Devoluções: {len(df_final[df_final['TIPO_MOV'] == 'DEVOLUCAO']):,} linhas")
 
    # Forçar TAMANHO como texto (resolve P, M, G)
    if 'TAMANHO' in df_final.columns:
        df_final['TAMANHO'] = df_final['TAMANHO'].astype(str)
        df_final['TAMANHO'] = "'" + df_final['TAMANHO'].fillna('')
   
    # ============================================================================
    # 9. SALVAR CSV (para upload ao Sheets)
    # ============================================================================
    print(f"\n💾 Salvando {CSV_PATH}...", end=" ", flush=True)
    
    try:
        df_final.to_csv(CSV_PATH, index=False, encoding='utf-8')
        
        tamanho_mb = os.path.getsize(CSV_PATH) / (1024 * 1024)
        duracao = (datetime.now() - inicio).total_seconds()
        
        print(f"✅ {tamanho_mb:.2f} MB em {duracao:.1f}s")
        
    except PermissionError:
        print(f"\n❌ Arquivo aberto! Feche e tente novamente.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        sys.exit(1)
    
    # ============================================================================
    # 10. SALVAR INFO DE EXECUÇÃO
    # ============================================================================
    save_last_run_info({
        'timestamp': inicio.isoformat(),
        'mode': 'incremental' if is_incremental else 'full',
        'records_processed': len(df_new),
        'total_records': len(df_final),
        'duration_seconds': duracao
    })
    
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