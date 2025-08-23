import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.gmail_manager import setup_gmail, get_emails
from functions.aws_manager import setup_aws
import re, base64

print("🔍 Analizando correos perdidos...")

service = setup_gmail()
try: cuentas_aws = setup_aws()
except: cuentas_aws = {}

mensajes = get_emails(service, "EST")
print(f"📧 {len(mensajes)} correos encontrados")

# Contadores detallados
procesados = 0
sin_cuenta = 0
sin_metrica = 0
sin_body = 0
duplicados = 0
insertados = 0
errores_decode = 0

def procesar_email_debug(msg_data, account_names):
    global sin_cuenta, sin_metrica, sin_body, errores_decode
    
    headers = {h['name']: h['value'] for h in msg_data['payload']['headers']}
    subject, fecha_raw = headers.get('Subject', ''), headers.get('Date', '')
    
    payload = msg_data['payload']
    body = ''
    
    # Extraer body con debug
    try:
        if 'parts' in payload:
            parts = [p for p in payload['parts'] if p['mimeType'] == 'text/plain']
            if parts and 'body' in parts[0] and 'data' in parts[0]['body']:
                body = base64.urlsafe_b64decode(parts[0]['body']['data']).decode('utf-8', errors='ignore')
        elif 'body' in payload and 'data' in payload['body']:
            body = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
    except Exception as e:
        errores_decode += 1
        print(f"⚠️ Error decodificando body: {e}")
    
    if not body:
        sin_body += 1
        return None
    
    # Extraer datos
    aws_account = re.search(r'AWS Account:\s+(\d+)', body)
    metric_name = re.search(r'MetricName:\s+([^\s,]+)', body)
    
    if not aws_account:
        sin_cuenta += 1
        print(f"❌ Sin cuenta AWS: {subject[:50]}...")
        return None
    
    if not metric_name:
        sin_metrica += 1
        print(f"❌ Sin métrica: {subject[:50]}...")
        return None
    
    return {
        'cuenta_id': aws_account.group(1),
        'metrica': metric_name.group(1),
        'fecha_str': fecha_raw
    }

# Procesar muestra de 200 correos para análisis
muestra = mensajes[:200]
print(f"🔬 Analizando muestra de {len(muestra)} correos...")

for i, msg in enumerate(muestra):
    try:
        msg_data = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
        resultado = procesar_email_debug(msg_data, cuentas_aws)
        procesados += 1
        
        if resultado:
            insertados += 1
        
        if i % 50 == 0:
            print(f"📊 Procesados: {i+1}/{len(muestra)}")
            
    except Exception as e:
        print(f"⚠️ Error procesando correo: {e}")
        continue

print(f"\n📈 RESULTADOS DEL ANÁLISIS:")
print(f"  📧 Correos procesados: {procesados}")
print(f"  ✅ Válidos para insertar: {insertados}")
print(f"  ❌ Sin cuenta AWS: {sin_cuenta}")
print(f"  ❌ Sin métrica: {sin_metrica}")
print(f"  ❌ Sin body: {sin_body}")
print(f"  ❌ Errores decode: {errores_decode}")
print(f"  📊 Tasa éxito: {(insertados/procesados*100):.1f}%")

# Proyección
perdidos_estimados = (len(mensajes) * (procesados - insertados)) // procesados
print(f"\n🔮 PROYECCIÓN TOTAL:")
print(f"  📧 Total correos: {len(mensajes)}")
print(f"  ✅ Insertables estimados: {len(mensajes) - perdidos_estimados}")
print(f"  ❌ Perdidos estimados: {perdidos_estimados}")
print(f"  📊 Tasa éxito estimada: {((len(mensajes) - perdidos_estimados)/len(mensajes)*100):.1f}%")