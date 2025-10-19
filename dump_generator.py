# dump_generator.py
"""
Gerador de scripts de dump para múltiplas tabelas do Firebird
Lê o schema.xlsx e cria scripts de dump personalizados
"""
import os
import sys
import pandas as pd
from datetime import datetime

SCHEMA_FILE = "schema.xlsx"
OUTPUT_SCRIPTS_DIR = "dump_scripts"

# Template base para os scripts
SCRIPT_TEMPLATE = '''# dump_{table_lower}.py
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
            print(f"→ Tentativa {{idx}}/{{len(CHARSETS)}}: charset={{cs}}...", end=" ", flush=True)
            conn = connect_with(cs)
            df = pd.read_sql(sql, conn)
            print(f"✅ Sucesso! ({{len(df)}} linhas)")
            return df, cs
        except KeyboardInterrupt:
            print("\\n⚠️ Interrompido pelo usuário (Ctrl+C)")
            sys.exit(1)
        except Exception as e:
            error_msg = str(e)
            if "op_code" in error_msg:
                error_msg = f"Erro de comunicação Firebird (op_code)"
            elif "transliteration" in error_msg.lower():
                error_msg = "Charset incompatível"
            print(f"❌ {{error_msg}}")
            last_error = e
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass
    raise Exception(f"Falhou com todos os charsets. Último erro: {{last_error}}")

def main():
    inicio = datetime.now()
    print("=" * 60)
    print("{icon} Iniciando dump de {table}")
    print("=" * 60)
    
    sql_full = "SELECT * FROM {table}"
    if LIMIT_ROWS:
        sql_full = f"SELECT FIRST {{LIMIT_ROWS}} * FROM {table}"
        print(f"⚠️ MODO TESTE: Limitado a {{LIMIT_ROWS}} linhas")
    
    try:
        print("\\n📊 Lendo dados da tabela {table}...")
        df, charset_usado = exec_sql(sql_full)
        
        print(f"\\n✅ Dados carregados com sucesso!")
        print(f"   • Charset: {{charset_usado}}")
        print(f"   • Linhas: {{len(df):,}}")
        print(f"   • Colunas: {{len(df.columns)}}")
        
        if len(df.columns) > 0:
            preview_cols = list(df.columns[:8])
            print(f"   • Primeiras colunas: {{', '.join(preview_cols)}}")
        
        if len(df) > 0 and len(df) <= 50:
            print(f"\\n📋 Preview dos dados:")
            print(df.head(10).to_string())
        elif len(df) > 0:
            print(f"\\n📋 Preview dos dados (5 primeiras linhas):")
            print(df.head(5).to_string())
            
    except KeyboardInterrupt:
        print("\\n⚠️ Operação cancelada pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\\n❌ ERRO FATAL ao ler {table}: {{e}}")
        sys.exit(1)
    
    xlsx_path = os.path.join(OUTPUT_DIR, "{table}_FULL.xlsx")
    
    print(f"\\n💾 Salvando em Excel...")
    print(f"   Arquivo: {{xlsx_path}}")
    
    try:
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="{table}")
        
        tamanho_mb = os.path.getsize(xlsx_path) / (1024 * 1024)
        print(f"\\n✅ Arquivo salvo com sucesso!")
        print(f"   • Tamanho: {{tamanho_mb:.2f}} MB")
        
    except PermissionError:
        print(f"\\n❌ ERRO: Permissão negada ao salvar {{xlsx_path}}")
        print(f"   → O arquivo está aberto no Excel? Feche-o e tente novamente.")
        sys.exit(1)
    except Exception as e:
        print(f"\\n❌ ERRO ao salvar Excel: {{e}}")
        sys.exit(1)
    
    fim = datetime.now()
    duracao = (fim - inicio).total_seconds()
    
    print("\\n" + "=" * 60)
    print(f"🏁 Concluído em {{duracao:.1f}} segundos")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\\n\\n⚠️ Processo interrompido pelo usuário")
        sys.exit(1)
'''

def get_icon_for_table(table_name: str) -> str:
    """Retorna um emoji baseado no nome da tabela."""
    table_lower = table_name.lower()
    
    icons = {
        'produto': '📦', 'marca': '🏷️', 'cliente': '👥', 'pedido': '🛒',
        'nf': '📄', 'nota': '📄', 'fiscal': '📄', 'compra': '🛍️',
        'venda': '💰', 'caixa': '💵', 'financ': '💳', 'pag': '💳',
        'estoque': '📊', 'moviment': '🔄', 'usuario': '👤', 'config': '⚙️',
        'os': '🔧', 'ordem': '🔧', 'servico': '🔧', 'veicul': '🚗',
        'fornecedor': '🏭', 'empresa': '🏢', 'loja': '🏪', 'banco': '🏦',
        'grade': '📏', 'tribut': '📋', 'imposto': '📋', 'digital': '⌨️',
        'marca': '🏷️', 'categoria': '📂', 'grupo': '📂'
    }
    
    for key, icon in icons.items():
        if key in table_lower:
            return icon
    
    return '📊'

def main():
    print("=" * 70)
    print("🚀 GERADOR DE SCRIPTS DE DUMP - Firebird")
    print("=" * 70)
    
    # Verifica se o schema existe
    if not os.path.exists(SCHEMA_FILE):
        print(f"\n❌ Arquivo '{SCHEMA_FILE}' não encontrado!")
        print("   → Certifique-se de ter o arquivo schema.xlsx no mesmo diretório")
        sys.exit(1)
    
    # Lê o schema
    print(f"\n📖 Lendo schema do banco...")
    try:
        df_schema = pd.read_excel(SCHEMA_FILE)
        tables = df_schema['table'].unique()
        tables = sorted(tables)
        
        print(f"✅ Schema carregado!")
        print(f"   • Total de tabelas: {len(tables)}")
        
    except Exception as e:
        print(f"❌ Erro ao ler schema: {e}")
        sys.exit(1)
    
    # Menu de opções
    print("\n" + "=" * 70)
    print("📋 OPÇÕES DE GERAÇÃO")
    print("=" * 70)
    print("1. Gerar script para UMA tabela específica")
    print("2. Gerar scripts para MÚLTIPLAS tabelas (você escolhe)")
    print("3. Gerar scripts para TODAS as tabelas")
    print("4. Ver lista de todas as tabelas")
    print("5. Buscar tabelas por nome")
    print("0. Sair")
    
    escolha = input("\n👉 Escolha uma opção: ").strip()
    
    if escolha == "0":
        print("👋 Até logo!")
        sys.exit(0)
    
    elif escolha == "4":
        print(f"\n📋 LISTA DE TODAS AS {len(tables)} TABELAS:")
        print("=" * 70)
        for idx, table in enumerate(tables, 1):
            cols = len(df_schema[df_schema['table'] == table])
            print(f"{idx:3d}. {table:<40} ({cols} colunas)")
        sys.exit(0)
    
    elif escolha == "5":
        busca = input("\n🔍 Digite parte do nome da tabela: ").strip().upper()
        encontradas = [t for t in tables if busca in t]
        
        if not encontradas:
            print(f"❌ Nenhuma tabela encontrada com '{busca}'")
            sys.exit(0)
        
        print(f"\n✅ {len(encontradas)} tabela(s) encontrada(s):")
        for idx, table in enumerate(encontradas, 1):
            cols = len(df_schema[df_schema['table'] == table])
            print(f"{idx}. {table} ({cols} colunas)")
        sys.exit(0)
    
    elif escolha == "1":
        # UMA tabela
        print(f"\n📋 Digite o nome da tabela (ou parte dele para buscar):")
        busca = input("👉 ").strip().upper()
        
        encontradas = [t for t in tables if busca in t]
        
        if not encontradas:
            print(f"❌ Nenhuma tabela encontrada com '{busca}'")
            sys.exit(1)
        
        if len(encontradas) > 1:
            print(f"\n✅ {len(encontradas)} tabelas encontradas:")
            for idx, table in enumerate(encontradas, 1):
                print(f"{idx}. {table}")
            
            idx_escolha = input("\n👉 Escolha o número da tabela: ").strip()
            try:
                table_escolhida = encontradas[int(idx_escolha) - 1]
            except:
                print("❌ Opção inválida!")
                sys.exit(1)
        else:
            table_escolhida = encontradas[0]
        
        tables_to_generate = [table_escolhida]
    
    elif escolha == "2":
        # MÚLTIPLAS tabelas
        print(f"\n📋 Digite os nomes das tabelas separados por vírgula:")
        print("   Exemplo: PRODUTOS, MARCAS, CLIENTES")
        entrada = input("👉 ").strip().upper()
        
        nomes = [n.strip() for n in entrada.split(',')]
        tables_to_generate = []
        
        for nome in nomes:
            encontradas = [t for t in tables if nome in t]
            if encontradas:
                if len(encontradas) == 1:
                    tables_to_generate.append(encontradas[0])
                else:
                    print(f"\n⚠️ Múltiplas tabelas encontradas para '{nome}':")
                    for idx, t in enumerate(encontradas, 1):
                        print(f"{idx}. {t}")
                    idx_escolha = input(f"👉 Escolha o número: ").strip()
                    try:
                        tables_to_generate.append(encontradas[int(idx_escolha) - 1])
                    except:
                        print(f"❌ Opção inválida para '{nome}', pulando...")
            else:
                print(f"❌ Tabela '{nome}' não encontrada, pulando...")
        
        if not tables_to_generate:
            print("❌ Nenhuma tabela válida selecionada!")
            sys.exit(1)
    
    elif escolha == "3":
        # TODAS as tabelas
        confirmacao = input(f"\n⚠️ Gerar scripts para TODAS as {len(tables)} tabelas? (s/N): ").strip().lower()
        if confirmacao != 's':
            print("❌ Operação cancelada")
            sys.exit(0)
        tables_to_generate = tables
    
    else:
        print("❌ Opção inválida!")
        sys.exit(1)
    
    # Cria o diretório de saída
    os.makedirs(OUTPUT_SCRIPTS_DIR, exist_ok=True)
    
    # Gera os scripts
    print(f"\n📝 Gerando {len(tables_to_generate)} script(s)...")
    print("=" * 70)
    
    for idx, table in enumerate(tables_to_generate, 1):
        table_lower = table.lower()
        script_name = f"dump_{table_lower}.py"
        script_path = os.path.join(OUTPUT_SCRIPTS_DIR, script_name)
        
        icon = get_icon_for_table(table)
        
        # Substitui os placeholders no template
        script_content = SCRIPT_TEMPLATE.format(
            table=table,
            table_lower=table_lower,
            icon=icon
        )
        
        # Salva o script
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(script_content)
        
        print(f"{idx:3d}. {icon} {script_name:<40} → {script_path}")
    
    print("\n" + "=" * 70)
    print(f"✅ {len(tables_to_generate)} script(s) gerado(s) com sucesso!")
    print(f"📁 Diretório: {OUTPUT_SCRIPTS_DIR}/")
    print("\n💡 Para executar um script:")
    print(f"   python {OUTPUT_SCRIPTS_DIR}/dump_<nome_tabela>.py")
    print("=" * 70)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Operação cancelada pelo usuário")
        sys.exit(1)