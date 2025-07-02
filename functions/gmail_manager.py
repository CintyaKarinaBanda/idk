import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from datetime import datetime
from config import GMAIL_CONFIG

def setup_gmail():
    creds = None
    # Define paths for credential files
    token_path = GMAIL_CONFIG["TOKEN_PATH"]
    credentials_path = GMAIL_CONFIG["CREDENTIALS_PATH"]
    
    # Buscar credenciales en múltiples ubicaciones posibles
    possible_credential_paths = [
        credentials_path,                      # Ruta configurada
        'credentials/credentials.json',        # Directorio actual/credentials
        '../credentials/credentials.json',     # Directorio superior/credentials
        '/home/ec2-user/credentials/credentials.json'  # Ruta absoluta en EC2
    ]
    
    # Encontrar la primera ruta válida
    valid_credentials_path = None
    for path in possible_credential_paths:
        if os.path.exists(path):
            valid_credentials_path = path
            print(f"✅ Credenciales encontradas en: {path}")
            break
    
    if not valid_credentials_path:
        print("❌ No se encontraron credenciales en ninguna ubicación conocida")
        print("Ubicaciones buscadas:")
        for path in possible_credential_paths:
            print(f"  - {path}")
        raise FileNotFoundError("No se encontró el archivo de credenciales")
    
    # Ensure credential directory exists for token
    os.makedirs(os.path.dirname(token_path), exist_ok=True)
    
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, GMAIL_CONFIG["SCOPES"])
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(valid_credentials_path, GMAIL_CONFIG["SCOPES"])
            creds = flow.run_local_server(port=0)
        with open(token_path, 'w') as token:
            token.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)

def get_emails(service, keyword, desde_hora=None):
    query = f"subject:{keyword}"
    if desde_hora:
        query += f" after:{desde_hora.strftime('%Y/%m/%d')}"
    return service.users().messages().list(userId='me', q=query).execute().get('messages', [])