import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.gmail_manager import setup_gmail, get_emails
from functions.aws_manager import setup_aws
import re, base64

print("🔍 Debug detallado de inserción...")

service = setup_gmail()
try: cuentas_aws = setup_aws()
except: cuentas_aws = {}

mensajes = get_emails(service, "EST")
print(f"📧 {len(mensajes)} correos encontrados")

def procesar_email_debug(msg_data, account_names):
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
        'fecha_str': fecha_raw,
        'subject': subject
    }

# Contadores detallados
total_procesados = 0
sin_cuenta_id = 0
cuenta_vacia = 0
metrica_vacia = 0
validos = 0

print("🔬 Analizando TODOS los correos...")

for i, msg in enumerate(mensajes):
    try:
        msg_data = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
        resultado = procesar_email_debug(msg_data, cuentas_aws)
        
        total_procesados += 1
        
        # Debug detallado
        if not resultado['cuenta_id']:
            sin_cuenta_id += 1
            if i < 5:  # Mostrar primeros 5 ejemplos
                print(f"❌ Sin cuenta_id: {resultado['subject'][:60]}")
        elif resultado['cuenta_id'] == '':
            cuenta_vacia += 1
            if cuenta_vacia <= 5:
                print(f"❌ Cuenta vacía: {resultado['subject'][:60]}")
        elif not resultado['metrica']:
            metrica_vacia += 1
            if metrica_vacia <= 5:
                print(f"❌ Sin métrica: {resultado['subject'][:60]}")
        else:
            validos += 1
        
        if i % 200 == 0:
            print(f"📊 Procesados: {i+1}/{len(mensajes)} - Válidos: {validos}")
            
    except Exception as e:
        print(f"⚠️ Error procesando correo {i}: {e}")
        continue

print(f"\n📈 RESULTADOS FINALES:")
print(f"  📧 Total procesados: {total_procesados}")
print(f"  ✅ Válidos para BD: {validos}")
print(f"  ❌ Sin cuenta_id: {sin_cuenta_id}")
print(f"  ❌ Cuenta vacía: {cuenta_vacia}")
print(f"  ❌ Sin métrica: {metrica_vacia}")
print(f"  📊 Perdidos totales: {total_procesados - validos}")
print(f"  🎯 Coincide con 79: {abs((total_procesados - validos) - 79) < 5}")

if validos != 1452:
    print(f"\n⚠️ DISCREPANCIA DETECTADA:")
    print(f"  Válidos calculados: {validos}")
    print(f"  Insertados reales: 1452")
    print(f"  Diferencia: {abs(validos - 1452)}")