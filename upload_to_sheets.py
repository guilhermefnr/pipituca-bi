"""
Upload de CSV para Google Sheets via Sheets API
Escreve diretamente nas células sem conversão de formato
"""
import os
import sys
import csv
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Configurações
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CSV_PATH = 'output/SAIDA_GRADE.csv'
SHEET_NAME = 'SAIDA_GRADE'  # Nome da aba

def main():
    print("\n" + "=" * 80)
    print("📤 UPLOAD PARA GOOGLE SHEETS")
    print("=" * 80)
    
    # Validar variáveis de ambiente
    creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    sheet_id = os.environ.get('SHEET_ID')
    
    if not creds_path:
        print("❌ GOOGLE_APPLICATION_CREDENTIALS não definida")
        sys.exit(1)
    
    if not sheet_id:
        print("❌ SHEET_ID não definida")
        sys.exit(1)
    
    # Verificar se CSV existe
    if not os.path.exists(CSV_PATH):
        print(f"❌ Arquivo não encontrado: {CSV_PATH}")
        sys.exit(1)
    
    print(f"📂 Arquivo: {CSV_PATH}")
    print(f"🔑 Sheet ID: {sheet_id}")
    print(f"📄 Aba: {SHEET_NAME}")
    
    # Autenticar
    print(f"\n🔐 Autenticando...", end=" ", flush=True)
    creds = service_account.Credentials.from_service_account_file(
        creds_path, scopes=SCOPES
    )
    service = build('sheets', 'v4', credentials=creds)
    print("✅")
    
    # Ler CSV
    print(f"📖 Lendo CSV...", end=" ", flush=True)
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        values = list(reader)
    print(f"✅ {len(values):,} linhas")
    
    # Limpar aba existente
    print(f"🧹 Limpando aba '{SHEET_NAME}'...", end=" ", flush=True)
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range=f"{SHEET_NAME}!A:ZZ"
        ).execute()
        print("✅")
    except Exception as e:
        print(f"⚠️  Aba não existe ou erro: {e}")
        print("   Tentando criar aba...")
        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={
                    'requests': [{
                        'addSheet': {
                            'properties': {'title': SHEET_NAME}
                        }
                    }]
                }
            ).execute()
            print("   ✅ Aba criada")
        except:
            pass  # Aba já existe com nome diferente
    
    # Escrever dados
    print(f"✍️  Escrevendo {len(values):,} linhas...", end=" ", flush=True)
    try:
        result = service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"{SHEET_NAME}!A1",
            valueInputOption='RAW',
            body={'values': values}
        ).execute()
        
        updated_cells = result.get('updatedCells', 0)
        print(f"✅ {updated_cells:,} células atualizadas")
        
    except Exception as e:
        print(f"\n❌ Erro ao escrever: {e}")
        sys.exit(1)
    
    # Formatação básica (cabeçalho em negrito)
    print(f"🎨 Formatando cabeçalho...", end=" ", flush=True)
    try:
        # Obter sheet_id da aba
        sheet_metadata = service.spreadsheets().get(
            spreadsheetId=sheet_id
        ).execute()
        
        sheet_gid = None
        for sheet in sheet_metadata.get('sheets', []):
            if sheet['properties']['title'] == SHEET_NAME:
                sheet_gid = sheet['properties']['sheetId']
                break
        
        if sheet_gid is not None:
            service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={
                    'requests': [
                        {
                            'repeatCell': {
                                'range': {
                                    'sheetId': sheet_gid,
                                    'startRowIndex': 0,
                                    'endRowIndex': 1
                                },
                                'cell': {
                                    'userEnteredFormat': {
                                        'textFormat': {'bold': True},
                                        'backgroundColor': {
                                            'red': 0.9,
                                            'green': 0.9,
                                            'blue': 0.9
                                        }
                                    }
                                },
                                'fields': 'userEnteredFormat(textFormat,backgroundColor)'
                            }
                        },
                        {
                            'autoResizeDimensions': {
                                'dimensions': {
                                    'sheetId': sheet_gid,
                                    'dimension': 'COLUMNS',
                                    'startIndex': 0,
                                    'endIndex': len(values[0]) if values else 10
                                }
                            }
                        }
                    ]
                }
            ).execute()
            print("✅")
        else:
            print("⚠️  Aba não encontrada para formatação")
            
    except Exception as e:
        print(f"⚠️  {e}")
    
    print("\n" + "=" * 80)
    print(f"🎉 Upload concluído com sucesso!")
    print(f"🔗 https://docs.google.com/spreadsheets/d/{sheet_id}")
    print("=" * 80)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrompido")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)