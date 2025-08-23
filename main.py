import os, re, base64, pandas as pd, pytz, yagmail
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from functions.aws_manager import setup_aws
from functions.gmail_manager import setup_gmail, get_emails 
from functions.excel_manager import generar_excel
from functions.db_manager import insertar_alertas, obtener_alertas_por_periodo
from config import EMAIL_CONFIG, REPORT_CONFIG

CST = pytz.timezone('America/Chicago')
PATRONES = ["cluster-stack (EKS)", "EMAWSBSDB01", "EMAWSCSDB03", "E2K6IWMA8DFJ3O Sitio www.estafeta.com", "alb-asg-WSFrecuency-prod-pub", "asg-WSFrecuency", "TG: tg-middlewareInvoice-pro-public del  ELB alb-middlewareInvoice-pro-public"]

def extraer_servicio(body):
    for p in PATRONES:
        if p in body: return p
    for pattern in [r'Alarm Name:\s*([^\n]+)', r'CloudWatch Alarm\s+"([^"]+)"']:
        match = re.search(pattern, body)
        if match:
            m = re.search(r'\*(?:Critical|Warning|Info)[^*]*\*\s*(.*)', match.group(1))
            if m: return m.group(1).strip()
    return ''

def procesar_email(msg_data, account_names, ahora, horas):
    headers = {h['name']: h['value'] for h in msg_data['payload']['headers']}
    subject, fecha_raw = headers.get('Subject', ''), headers.get('Date', '')
    
    try:
        fecha_dt = parsedate_to_datetime(fecha_raw).astimezone()
        if horas and fecha_dt < ahora - timedelta(hours=horas): return None
        fecha_str = fecha_dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        if horas: return None
        fecha_str = fecha_raw
    
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
    
    return {
        'Id cuenta': aws_account.group(1) if aws_account else '',
        'Nombre cuenta': account_names.get(aws_account.group(1) if aws_account else '', 'Desconocido'),
        'Metrica': metric_name.group(1) if metric_name else '',
        'Servicio': extraer_servicio(body),
        'Namespace': namespace.group(1) if namespace else '',
        'Estado': estado,
        'Fecha': fecha_str
    }

def analizar_mensajes(service, messages, account_names, horas=None):
    ahora = datetime.now(CST)
    if horas: print(f"🔍 Filtro: últimas {horas}h")
    
    data, excluidos = [], 0
    for msg in messages:
        resultado = procesar_email(service.users().messages().get(userId='me', id=msg['id'], format='full').execute(), account_names, ahora, horas)
        if resultado: data.append(resultado)
        else: excluidos += 1
    
    print(f"📊 {len(data)} incluidos, {excluidos} excluidos")
    return pd.DataFrame(data)

def crear_mensaje_correo(periodo, horas, df):
    fecha, num = datetime.now().strftime('%Y-%m-%d'), len(df)
    configs = {
        'mensual': (f"Concentrado mensual de alarmas Estafeta: {fecha}", "mensual"),
        'custom': (f"Concentrado de alarmas Estafeta: {num} alertas en últimas {horas}h ({fecha})", f"de las últimas {horas} horas")
    }
    subject, detalle = configs.get(periodo, (f"Concentrado de alarmas Estafeta: {num} alertas - {periodo.capitalize()} ({fecha})", periodo))
    return subject, f"Buen día,\n\nSe adjunta el concentrado {detalle} de alarmas de Estafeta.\n\nSe detectaron {num} alertas en este periodo.\n\nEste es un mensaje automático.\nPor favor, no responda a este correo.\n\nSaludos cordiales."

def generar_reporte(service, keyword, periodo='diario', horas=None):
    print(f"📊 {periodo.upper()} ({horas or 'Días'})")
    
    try:
        account_names = setup_aws()
    except:
        account_names = {}
    
    if service and periodo in ['custom', 'diario'] and periodo != 'mensual':
        desde = datetime.now(CST) - (timedelta(hours=horas) if horas else timedelta(days=1))
        df = analizar_mensajes(service, get_emails(service, keyword, desde), account_names, horas)
        if not df.empty: insertar_alertas(df)
    else:
        df = obtener_alertas_por_periodo(periodo, horas)
        if df.empty and not service:
            df = pd.DataFrame([{'Id cuenta': '123456789012', 'Nombre cuenta': 'Cuenta Ejemplo', 'Metrica': 'CPUUtilization', 'Servicio': 'Servicio Ejemplo', 'Namespace': 'AWS/EC2', 'Estado': 'Warning', 'Fecha': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}])

    
    resumen = pd.DataFrame()
    if not df.empty:
        try:
            resumen = df.groupby(['Id cuenta', 'Nombre cuenta', 'Metrica', 'Servicio', 'Estado']).size().reset_index(name='Cantidad').pivot_table(index=['Id cuenta', 'Nombre cuenta', 'Metrica', 'Servicio'], columns='Estado', values='Cantidad', fill_value=0).reset_index().sort_values(['Id cuenta', 'Nombre cuenta', 'Metrica'])
            for estado in ['Critica', 'Warning', 'Informativo']:
                if estado not in resumen.columns: resumen[estado] = 0
            resumen['Total'] = resumen[['Critica', 'Warning', 'Informativo']].sum(axis=1)
            resumen = resumen.sort_values('Total', ascending=False)
        except: pass
    
    excel_generado = generar_excel(df, resumen, periodo, horas)
    print(f"✅ {len(df)} alertas")
    print(f"📄 Excel generado: {excel_generado}")
    
    try:
        subject, message = crear_mensaje_correo(periodo, horas, df)
        attachments = []
        if excel_generado:
            archivo = os.path.join(os.getcwd(), REPORT_CONFIG["EXCEL_DIR"], f'Alertas_{periodo}{f"_ultimas_{horas}h" if horas else ""}.xlsx')
            print(f"📎 Archivo: {archivo}")
            if os.path.exists(archivo):
                attachments = [archivo]
                print(f"✅ Archivo existe, adjuntando")
            else:
                print(f"⚠️ Archivo no existe")
        else:
            print(f"⚠️ Excel no generado, enviando sin adjunto")
        yagmail.SMTP(EMAIL_CONFIG["REMITENTE"], EMAIL_CONFIG["PASSWORD"]).send(to=EMAIL_CONFIG["DESTINATARIO"], subject=subject, contents=message, cc=EMAIL_CONFIG["COPIAS"], attachments=attachments)
        print(f"✅ Enviado a {EMAIL_CONFIG['DESTINATARIO']}")
    except Exception as e:
        print(f"⚠️ Error envío: {e}")

def main(periodo, keyword=REPORT_CONFIG["DEFAULT_KEYWORD"], horas_custom=None):
    print(f"\n📈 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try: service = setup_gmail()
    except: service = None
    horas = horas_custom or (REPORT_CONFIG["HORAS_CUSTOM"] if periodo == 'custom' else None)
    generar_reporte(service, keyword, periodo, horas)
    print("✅ Completado")
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--periodo', default="mensual", choices=["diario", "semanal", "mensual", "custom"])
    parser.add_argument('--keyword', default=REPORT_CONFIG["DEFAULT_KEYWORD"])
    parser.add_argument('--horas', type=int)
    args = parser.parse_args()
    main(args.periodo, args.keyword, args.horas)