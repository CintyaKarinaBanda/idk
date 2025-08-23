import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.gmail_manager import setup_gmail, get_emails
from functions.aws_manager import setup_aws
from functions.db_manager import get_connection
import re, base64, pandas as pd
from datetime import datetime

print("🚀 Insertando TODOS los correos EST...")

# Setup
service = setup_gmail()
try: cuentas_aws = setup_aws()
except: cuentas_aws = {}

# Obtener TODOS los correos EST (sin filtro de fecha)
mensajes = get_emails(service, "EST")
print(f"📧 {len(mensajes)} correos encontrados")

def procesar_email(msg_data, account_names):
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
    
    estado = 'Critica' if 'critical' in subject.lower() else 'Warning' if 'warning' in subject.lower() else 'Informativo' if 'info' in subject.lower() else 'Desconocido'
    aws_account = re.search(r'AWS Account:\s+(\d+)', body)
    metric_name = re.search(r'MetricName:\s+([^\s,]+)', body)
    namespace = re.search(r'Namespace:\s+([^\s,]+)', body)
    
    # Extraer servicio
    servicio = ''
    patrones = ["cluster-stack (EKS)", "EMAWSBSDB01", "EMAWSCSDB03", "E2K6IWMA8DFJ3O Sitio www.estafeta.com", "alb-asg-WSFrecuency-prod-pub", "asg-WSFrecuency", "TG: tg-middlewareInvoice-pro-public del  ELB alb-middlewareInvoice-pro-public"]
    for p in patrones:
        if p in body: 
            servicio = p
            break
    if not servicio:
        for pattern in [r'Alarm Name:\s*([^\n]+)', r'CloudWatch Alarm\s+"([^"]+)"']:
            match = re.search(pattern, body)
            if match:
                m = re.search(r'\*(?:Critical|Warning|Info)[^*]*\*\s*(.*)', match.group(1))
                if m: 
                    servicio = m.group(1).strip()
                    break
    
    return {
        'cuenta_id': aws_account.group(1) if aws_account else '',
        'cuenta_nombre': account_names.get(aws_account.group(1) if aws_account else '', 'Desconocido'),
        'metrica': metric_name.group(1) if metric_name else '',
        'servicio': servicio,
        'namespace': namespace.group(1) if namespace else '',
        'estado': estado,
        'fecha_str': fecha_raw
    }

# Procesar en lotes de 100
conn = get_connection()
cursor = conn.cursor()
inserted = 0
batch_size = 100

for i in range(0, len(mensajes), batch_size):
    batch = mensajes[i:i+batch_size]
    print(f"🔄 Procesando lote {i//batch_size + 1}/{(len(mensajes)-1)//batch_size + 1} ({len(batch)} correos)")
    
    for msg in batch:
        try:
            msg_data = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
            resultado = procesar_email(msg_data, cuentas_aws)
            
            if resultado['cuenta_id']:
                cursor.execute("SELECT 1 FROM alertas WHERE cuenta_id=%s AND metrica=%s AND servicio=%s AND estado=%s AND fecha_str=%s", 
                             (resultado['cuenta_id'], resultado['metrica'], resultado['servicio'], resultado['estado'], resultado['fecha_str']))
                if not cursor.fetchone():
                    cursor.execute("INSERT INTO alertas (cuenta_id, cuenta_nombre, metrica, servicio, namespace, estado, fecha_str) VALUES (%s,%s,%s,%s,%s,%s,%s)", 
                                 (resultado['cuenta_id'], resultado['cuenta_nombre'], resultado['metrica'], resultado['servicio'], resultado['namespace'], resultado['estado'], resultado['fecha_str']))
                    inserted += 1
        except Exception as e:
            print(f"⚠️ Error: {e}")
            continue
    
    conn.commit()
    print(f"✅ Lote completado. Total insertadas: {inserted}")

conn.close()
print(f"🎉 Proceso completado: {inserted} alertas insertadas de {len(mensajes)} correos")