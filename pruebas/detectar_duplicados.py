import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.gmail_manager import setup_gmail, get_emails
from functions.aws_manager import setup_aws
import re, base64
from collections import defaultdict

print("🔍 Analizando duplicados en correos...")

service = setup_gmail()
try: cuentas_aws = setup_aws()
except: cuentas_aws = {}

mensajes = get_emails(service, "EST")
print(f"📧 {len(mensajes)} correos encontrados")

def extraer_datos_clave(msg_data):
    headers = {h['name']: h['value'] for h in msg_data['payload']['headers']}
    subject, fecha_raw = headers.get('Subject', ''), headers.get('Date', '')
    
    payload = msg_data['payload']
    body = ''
    if 'parts' in payload:
        parts = [p for p in payload['parts'] if p['mimeType'] == 'text/plain']
        if parts and 'body' in parts[0] and 'data' in parts[0]['body']:
            body = base64.urlsafe_b64decode(parts[0]['body']['data']).decode('utf-8', errors='ignore')
    elif 'body' in payload and 'data' in payload['body']:
        body = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
    
    aws_account = re.search(r'AWS Account:\s+(\d+)', body)
    metric_name = re.search(r'MetricName:\s+([^\s,]+)', body)
    
    return {
        'cuenta_id': aws_account.group(1) if aws_account else '',
        'metrica': metric_name.group(1) if metric_name else '',
        'fecha_str': fecha_raw,
        'subject': subject
    }

# Diccionario para detectar duplicados
duplicados_map = defaultdict(list)
procesados = 0

print("🔬 Procesando correos para detectar duplicados...")

# Procesar muestra de 300 correos
muestra = mensajes[:300]
for i, msg in enumerate(muestra):
    try:
        msg_data = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
        datos = extraer_datos_clave(msg_data)
        
        if datos['cuenta_id'] and datos['metrica']:
            # Crear clave única basada en los campos del constraint
            clave = (datos['cuenta_id'], datos['metrica'], datos['fecha_str'])
            duplicados_map[clave].append({
                'index': i,
                'subject': datos['subject'][:60],
                'fecha': datos['fecha_str']
            })
        
        procesados += 1
        if i % 50 == 0:
            print(f"📊 Procesados: {i+1}/{len(muestra)}")
            
    except Exception as e:
        continue

# Analizar duplicados
duplicados_encontrados = 0
duplicados_reales = 0

print(f"\n🔍 ANÁLISIS DE DUPLICADOS:")
for clave, correos in duplicados_map.items():
    if len(correos) > 1:
        duplicados_encontrados += len(correos) - 1
        duplicados_reales += 1
        print(f"\n📧 DUPLICADO #{duplicados_reales}:")
        print(f"  🔑 Cuenta: {clave[0]}, Métrica: {clave[1]}")
        print(f"  📅 Fecha: {clave[2]}")
        print(f"  📊 Cantidad: {len(correos)} correos")
        for correo in correos:
            print(f"    - {correo['subject']}")

print(f"\n📈 RESUMEN:")
print(f"  📧 Correos analizados: {procesados}")
print(f"  🔄 Grupos duplicados: {duplicados_reales}")
print(f"  ❌ Correos duplicados: {duplicados_encontrados}")
print(f"  ✅ Correos únicos: {procesados - duplicados_encontrados}")
print(f"  📊 % Duplicados: {(duplicados_encontrados/procesados*100):.1f}%")

# Proyección al total
if procesados > 0:
    tasa_duplicados = duplicados_encontrados / procesados
    duplicados_totales_estimados = int(len(mensajes) * tasa_duplicados)
    print(f"\n🔮 PROYECCIÓN TOTAL:")
    print(f"  📧 Total correos: {len(mensajes)}")
    print(f"  ❌ Duplicados estimados: {duplicados_totales_estimados}")
    print(f"  ✅ Únicos estimados: {len(mensajes) - duplicados_totales_estimados}")
    print(f"  📊 Coincide con inserción: {abs(duplicados_totales_estimados - 79) < 20}")