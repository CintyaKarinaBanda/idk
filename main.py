import os
import re
import base64
import pandas as pd
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
import yagmail
from functions.aws_manager import setup_aws
from functions.gmail_manager import setup_gmail, get_emails 
from functions.excel_manager import generar_excel
from functions.db_manager import insertar_alertas, obtener_alertas_por_periodo
from config import EMAIL_CONFIG, REPORT_CONFIG

HORAS_CUSTOM = REPORT_CONFIG["HORAS_CUSTOM"]

def analizar_mensajes(service, messages, account_names, horas=None):
    data = []
    ahora = datetime.now()

    for msg in messages:
        msg_data = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
        headers = {h['name']: h['value'] for h in msg_data['payload']['headers']}
        subject = headers.get('Subject', '')
        fecha_raw = headers.get('Date', '')

        try:
            fecha_dt = parsedate_to_datetime(fecha_raw).astimezone()
            # Filtro exacto: solo incluir si está dentro del rango de horas
            if horas:
                limite_tiempo = ahora - timedelta(hours=horas)
                if fecha_dt < limite_tiempo:
                    continue
            fecha_str = fecha_dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            # Si no se puede parsear la fecha y hay filtro de horas, saltar
            if horas:
                continue
            fecha_str = fecha_raw
            fecha_dt = None

        # Extraer cuerpo del mensaje
        body = ''
        payload = msg_data['payload']
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
        servicio_recurso = ''
        patrones_servicio = ["cluster-stack (EKS)", "EMAWSBSDB01", "EMAWSCSDB03", "E2K6IWMA8DFJ3O", "alb-asg-WSFrecuency"]
        
        for patron in patrones_servicio:
            if patron in body:
                servicio_recurso = patron
                break
        
        # Si no se encontró ninguno de los patrones predefinidos, intentar extraer del cuerpo
        if not servicio_recurso:
            # Patrón 1: Buscar en el cuerpo el nombre completo de la alarma
            alarm_name_match = re.search(r'Alarm Name:\s*([^\n]+)', body)
            if alarm_name_match:
                full_alarm = alarm_name_match.group(1).strip()
                # Extraer la parte después de los asteriscos
                match = re.search(r'\*(?:Critical|Warning|Info)[^*]*\*\s*(.*)', full_alarm)
                if match:
                    servicio_recurso = match.group(1).strip()
            
            # Patrón 2: Buscar en el cuerpo menciones a CloudWatch Alarm
            if not servicio_recurso:
                alarm_match = re.search(r'CloudWatch Alarm\s+"([^"]+)"', body)
                if alarm_match:
                    full_alarm = alarm_match.group(1).strip()
                    # Extraer la parte después de los asteriscos
                    match = re.search(r'\*(?:Critical|Warning|Info)[^*]*\*\s*(.*)', full_alarm)
                    if match:
                        servicio_recurso = match.group(1).strip()
        
        account_id = aws_account.group(1) if aws_account else ''
        
        data.append({
            'Id cuenta': account_id,
            'Nombre cuenta': account_names.get(account_id, 'Desconocido'),
            'Metrica': metric_name.group(1) if metric_name else '',
            'Servicio': servicio_recurso,
            'Namespace': namespace.group(1) if namespace else '',
            'Estado': estado,
            'Fecha': fecha_dt,
            'Fecha_str': fecha_str
        })

    return pd.DataFrame(data)

def crear_mensaje_correo(periodo, horas, df):
    fecha_actual = datetime.now().strftime('%Y-%m-%d')
    if periodo == 'mensual':
        subject = f"Concentrado mensual de alarmas Estafeta: {fecha_actual}"
        detalle = f"Se adjunta el concentrado mensual de alarmas de Estafeta.\n\n"
    elif periodo == 'custom':
        subject = f"Concentrado de alarmas Estafeta: {len(df)} alertas en últimas {horas}h ({fecha_actual})"
        detalle = f"Se adjunta el concentrado de alarmas de Estafeta de las últimas {horas} horas.\n"
    else:
        subject = f"Concentrado de alarmas Estafeta: {len(df)} alertas - {periodo.capitalize()} ({fecha_actual})"
        detalle = f"Se adjunta el concentrado {periodo} de alarmas de Estafeta.\n"
    
    message = "Buen día,\n\n"
    message += detalle
    
    if periodo != 'mensual':
        message += f"Se detectaron {len(df)} alertas en este periodo.\n\n"
        
    message += "\nEste es un mensaje automático. \n"
    message += "Por favor, no responda a este correo.\n\n"
    message += "Saludos cordiales."
    
    return subject, message

def generar_reporte(service, keyword, periodo='diario', horas=None):
    print(f"📊 Generando reporte: {periodo.upper()} ({horas or 'Días'})")

    try:
        account_names = setup_aws()
        
        if (periodo == 'custom' or periodo == 'diario') and service is not None:
            ahora = datetime.now()
            if periodo == 'custom' and horas:
                desde = ahora - timedelta(hours=horas)
                print(f"🕐 Filtrando alertas desde: {desde.strftime('%Y-%m-%d %H:%M:%S')} hasta: {ahora.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                desde = ahora - timedelta(days=1)
                
            messages = get_emails(service, keyword, desde)
            print(f"📧 Mensajes obtenidos: {len(messages)}")
            
            df = analizar_mensajes(service, messages, account_names, horas)
            print(f"📊 DataFrame inicial: {len(df)} filas")
            
            # Filtro adicional para periodo custom
            if not df.empty and periodo == 'custom' and horas:
                antes_filtro = len(df)
                df = df[df['Fecha'] >= desde]
                print(f"📅 Alertas antes del filtro: {antes_filtro}, después: {len(df)}")
            
            if not df.empty:
                print(f"💾 Insertando {len(df)} alertas en BD")
                insertar_alertas(df)
            else:
                print("⚠️ No hay alertas para insertar")
        else:
            print("📊 Obteniendo alertas de la base de datos")
            df = obtener_alertas_por_periodo(periodo, horas)
            print(f"💾 Alertas de BD: {len(df)}")
            
        # Si no hay datos, crear DataFrame vacío con estructura correcta
        if df.empty:
            print("⚠️ No hay alertas, creando DataFrame vacío")
            df = pd.DataFrame({
                'Id cuenta': [],
                'Nombre cuenta': [],
                'Metrica': [],
                'Servicio': [],
                'Namespace': [],
                'Estado': [],
                'Fecha': [],
                'Fecha_str': []
            })

        resumen = pd.DataFrame()
        if not df.empty:
            try:
                # Agrupar primero por cuenta y luego por métrica para el orden deseado
                resumen = (
                    df.groupby(['Id cuenta', 'Nombre cuenta', 'Metrica', 'Servicio', 'Estado'])
                    .size().reset_index(name='Cantidad')
                    .pivot_table(index=['Id cuenta', 'Nombre cuenta', 'Metrica', 'Servicio'], 
                                columns='Estado', values='Cantidad', fill_value=0)
                    .reset_index()
                )
                
                # Ordenar primero por cuenta y luego por métrica
                resumen = resumen.sort_values(['Id cuenta', 'Nombre cuenta', 'Metrica'])
                
                for estado in ['Critica', 'Warning', 'Informativo']:
                    if estado not in resumen.columns:
                        resumen[estado] = 0
                        
                resumen['Total'] = resumen[['Critica', 'Warning', 'Informativo']].sum(axis=1)
                resumen = resumen.sort_values('Total', ascending=False)
            except Exception as e:
                print(f"Error al crear resumen: {e}")

        print(f"📈 Generando Excel con {len(df)} alertas y {len(resumen)} filas de resumen")
        if generar_excel(df, resumen, periodo, horas):
            print(f"✅ {len(df)} alertas exportadas")
        else:
            print("⚠️ Problemas al generar el reporte")
            return
    except Exception as e:
        print(f"❌ Error: {e}")
        return

    try:
        sufijo = f"_ultimas_{horas}h" if horas else ""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre_archivo = f'Alertas_{periodo}{sufijo}_{timestamp}.xlsx'
        archivo_adjunto = os.path.join(os.getcwd(), REPORT_CONFIG["EXCEL_DIR"], nombre_archivo)
        
        subject, message = crear_mensaje_correo(periodo, horas, df)
        
        yag = yagmail.SMTP(EMAIL_CONFIG["REMITENTE"], EMAIL_CONFIG["PASSWORD"])
        yag.send(
            to=EMAIL_CONFIG["DESTINATARIO"],
            subject=subject,
            contents=message,
            cc=EMAIL_CONFIG["COPIAS"],
            attachments=archivo_adjunto
        )
        print(f"✅ Reporte enviado a {EMAIL_CONFIG['DESTINATARIO']} y {len(EMAIL_CONFIG['COPIAS'])} destinatarios en copia")
    except Exception as e:
        print(f"⚠️ Error al enviar el correo: {e}")

def main(periodo, keyword=REPORT_CONFIG["DEFAULT_KEYWORD"], horas_custom=None):
    print("\n📈 === MONITOREO Y PERSPECTIVA AVANZADA ===")
    print(f'Empezado: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    # Simplemente continuar con el proceso sin verificaciones adicionales
    
    # Intentar configurar Gmail, pero continuar si falla
    try:
        service = setup_gmail()
    except Exception as e:
        print(f"⚠️ Error al configurar Gmail: {e}")
        print("ℹ️ Continuando sin acceso a Gmail (usando datos de la base de datos)")
        service = None
    
    # Determinar las horas para el periodo personalizado
    horas = horas_custom if horas_custom else HORAS_CUSTOM if periodo == 'custom' else None
    
    if periodo == 'custom' and horas:
        print(f"⏰ Configurado para últimas {horas} horas exactas")
    
    generar_reporte(service, keyword, periodo, horas)

    print("✅ Reportes generados con éxito. Incluyen")

def actualizar_horas_custom(horas):
    global HORAS_CUSTOM
    HORAS_CUSTOM = horas

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Genera reportes de alertas de AWS')
    parser.add_argument('--periodo', 
                      default="mensual",
                      choices=["diario", "semanal", "mensual", "custom"],
                      help='Periodo a generar (diario, semanal, mensual, custom)')
    parser.add_argument('--keyword', default=REPORT_CONFIG["DEFAULT_KEYWORD"],
                      help='Palabra clave para buscar en los correos')
    parser.add_argument('--horas', type=int, default=None,
                      help='Número de horas para el periodo custom')
    
    args = parser.parse_args()
    main(args.periodo, args.keyword, args.horas)