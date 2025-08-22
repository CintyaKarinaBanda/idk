import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.gmail_manager import setup_gmail, get_emails
from functions.aws_manager import setup_aws
from functions.db_manager import get_engine
from main import analizar_mensajes
import pandas as pd
from datetime import datetime

hoy = datetime.now()
inicio_julio = datetime(2025, 7, 1, 0, 0, 0, 0)

print(f"🗓️ Buscando desde: {inicio_julio.strftime('%d/%m/%Y %H:%M')}")
print(f"🗓️ Hasta hoy: {hoy.strftime('%d/%m/%Y %H:%M')}")

try:
    service = setup_gmail()
    mensajes = get_emails(service, "EST", inicio_julio, hoy)
    print(f"📧 {len(mensajes)} correos Gmail")
except:
    service = None
    mensajes = []
    print("⚠️ Gmail no disponible")

try:
    cuentas_aws = setup_aws()
except:
    cuentas_aws = {}

# Consulta personalizada para dos meses desde julio
query_dos_meses = "SELECT cuenta_id as \"Id cuenta\", cuenta_nombre as \"Nombre cuenta\", metrica as \"Metrica\", servicio as \"Servicio\", namespace as \"Namespace\", estado as \"Estado\", fecha_str as \"Fecha\" FROM alertas WHERE fecha_str::date >= '2025-07-01' AND fecha_str::date <= CURRENT_DATE"
df_bd = pd.read_sql_query(query_dos_meses, get_engine())
print(f"🗄️ {len(df_bd)} alertas en BD (julio-agosto)")

print(f"📊 Comparación: {len(mensajes)} correos Gmail vs {len(df_bd)} alertas BD")
if len(mensajes) > len(df_bd):
    print(f"⚠️ Faltan {len(mensajes) - len(df_bd)} alertas en BD")
    
    if service and mensajes:
        print("🔄 Procesando correos para insertar faltantes...")
        # Crear función temporal que preserve formato original de fecha

        import re, base64
        from email.utils import parsedate_to_datetime
        
        def procesar_email_original(msg_data, account_names):
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
                'Id cuenta': aws_account.group(1) if aws_account else '',
                'Nombre cuenta': account_names.get(aws_account.group(1) if aws_account else '', 'Desconocido'),
                'Metrica': metric_name.group(1) if metric_name else '',
                'Servicio': servicio,
                'Namespace': namespace.group(1) if namespace else '',
                'Estado': estado,
                'Fecha': fecha_raw  # Mantener formato original
            }
        
        data = []
        for msg in mensajes:
            resultado = procesar_email_original(service.users().messages().get(userId='me', id=msg['id'], format='full').execute(), cuentas_aws)
            if resultado: data.append(resultado)
        
        df_nuevas = pd.DataFrame(data)
        print(f"📊 {len(df_nuevas)} correos procesados")
        
        # Inserción con manejo de errores mejorado
        from functions.db_manager import get_connection
        if not df_nuevas.empty:
            df_db = df_nuevas.rename(columns={'Id cuenta': 'cuenta_id', 'Nombre cuenta': 'cuenta_nombre', 'Metrica': 'metrica', 'Servicio': 'servicio', 'Namespace': 'namespace', 'Estado': 'estado', 'Fecha': 'fecha_str'})
            
            conn = get_connection()
            cursor = conn.cursor()
            inserted = 0
            
            try:
                for _, row in df_db.iterrows():
                    try:
                        cursor.execute("SELECT 1 FROM alertas WHERE cuenta_id=%s AND metrica=%s AND servicio=%s AND estado=%s AND fecha_str=%s", (row['cuenta_id'], row['metrica'], row['servicio'], row['estado'], row['fecha_str']))
                        if not cursor.fetchone():
                            cursor.execute("INSERT INTO alertas (cuenta_id, cuenta_nombre, metrica, servicio, namespace, estado, fecha_str) VALUES (%s,%s,%s,%s,%s,%s,%s)", tuple(row))
                            inserted += 1
                    except Exception as e:
                        conn.rollback()
                        print(f"⚠️ Error en registro: {e}")
                        continue
                
                conn.commit()
                print(f"✅ {inserted} alertas insertadas")
            except Exception as e:
                conn.rollback()
                print(f"❌ Error en inserción: {e}")
            finally:
                conn.close()
        else:
            print("❌ No hay datos para insertar")
    else:
        print("❌ No se pueden procesar correos sin Gmail")
elif len(mensajes) < len(df_bd):
    print(f"ℹ️ BD tiene {len(df_bd) - len(mensajes)} alertas más que Gmail")
else:
    print("✅ Gmail y BD están sincronizados")