import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.gmail_manager import setup_gmail, get_emails
from functions.aws_manager import setup_aws
from functions.db_manager import get_connection
import re, base64

print("🔍 Debug inserción BD...")

service = setup_gmail()
try: cuentas_aws = setup_aws()
except: cuentas_aws = {}

# Tomar muestra de 100 correos para debug detallado
mensajes = get_emails(service, "EST")[:100]
print(f"📧 Analizando {len(mensajes)} correos")

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

conn = get_connection()
cursor = conn.cursor()

# Contadores detallados
procesados = 0
validos = 0
duplicados = 0
insertados = 0
errores_sql = 0
errores_constraint = 0
errores_otros = 0

for i, msg in enumerate(mensajes):
    try:
        msg_data = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
        resultado = procesar_email(msg_data, cuentas_aws)
        procesados += 1
        
        if resultado['cuenta_id']:
            validos += 1
            
            try:
                # Verificar duplicado
                cursor.execute("SELECT 1 FROM alertas WHERE cuenta_id=%s AND metrica=%s AND servicio=%s AND estado=%s AND fecha_str=%s", 
                             (resultado['cuenta_id'], resultado['metrica'], resultado['servicio'], resultado['estado'], resultado['fecha_str']))
                
                if cursor.fetchone():
                    duplicados += 1
                    print(f"🔄 Duplicado: {resultado['cuenta_id']} - {resultado['metrica']}")
                else:
                    # Intentar insertar
                    try:
                        cursor.execute("INSERT INTO alertas (cuenta_id, cuenta_nombre, metrica, servicio, namespace, estado, fecha_str) VALUES (%s,%s,%s,%s,%s,%s,%s)", 
                                     (resultado['cuenta_id'], resultado['cuenta_nombre'], resultado['metrica'], resultado['servicio'], resultado['namespace'], resultado['estado'], resultado['fecha_str']))
                        insertados += 1
                        conn.commit()
                    except Exception as insert_error:
                        conn.rollback()
                        error_msg = str(insert_error)
                        if "unique constraint" in error_msg.lower():
                            errores_constraint += 1
                            print(f"⚠️ Constraint: {resultado['cuenta_id']} - {resultado['metrica']}")
                        else:
                            errores_sql += 1
                            print(f"❌ SQL Error: {error_msg[:100]}")
                            
            except Exception as check_error:
                errores_otros += 1
                print(f"❌ Check Error: {str(check_error)[:100]}")
                conn.rollback()
                
    except Exception as e:
        print(f"⚠️ Error procesando: {str(e)[:100]}")
        continue

conn.close()

print(f"\n📈 RESULTADOS DETALLADOS:")
print(f"  📧 Procesados: {procesados}")
print(f"  ✅ Válidos: {validos}")
print(f"  🔄 Duplicados: {duplicados}")
print(f"  ✅ Insertados: {insertados}")
print(f"  ⚠️ Errores constraint: {errores_constraint}")
print(f"  ❌ Errores SQL: {errores_sql}")
print(f"  ❌ Otros errores: {errores_otros}")
print(f"  📊 Total perdidos: {validos - insertados}")
print(f"  📊 Tasa inserción: {(insertados/validos*100):.1f}%")