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
from functions.db_manager import crear_tabla_si_no_existe, insertar_alertas, obtener_alertas_por_periodo
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
            if horas and fecha_dt < ahora - timedelta(hours=horas):
                continue
            fecha_str = fecha_dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            fecha_str = fecha_raw
            fecha_dt = None

        payload = msg_data['payload']
        if 'parts' in payload:
            parts = [p for p in payload['parts'] if p['mimeType'] == 'text/plain']
            body = base64.urlsafe_b64decode(parts[0]['body']['data']).decode() if parts else ''
        else:
            body = base64.urlsafe_b64decode(payload['body']['data']).decode()

        estado = 'Critica' if 'critical' in subject.lower() else 'Warning' if 'warning' in subject.lower() else 'Informativo' if 'info' in subject.lower() else 'Desconocido'
        
        aws_account = re.search(r'AWS Account:\s+(\d+)', body)
        metric_name = re.search(r'MetricName:\s+([^\s,]+)', body)
        region = re.search(r'Region:\s+([^\s,]+)', body)
        namespace = re.search(r'Namespace:\s+([^\s,]+)', body)
        reason = re.search(r'NewStateReason:\s+([^\n]+)', body)
        
        # Extraer servicio/recurso del cuerpo del mensaje
        servicio_recurso = ''
        
        # Buscar patrones en el cuerpo
        # Patrón 1: "Alarm Name: Est-FreCot *Critical* HTTPCode_ELB_5XX_Count del ELB: alb-asg-WSFrecuency-prod-pub"
        alarm_name_match = re.search(r'Alarm Name:\s*([^\n]+)', body)
        if alarm_name_match:
            servicio_recurso = alarm_name_match.group(1).strip()
        
        # Patrón 2: "your Amazon CloudWatch Alarm "Est-Multi *Warning Memory % Committed Bytes In Use* EMAWSBSDB01 (DB Windows 2016)"
        if not servicio_recurso:
            alarm_match = re.search(r'CloudWatch Alarm\s+"([^"]+)"', body)
            if alarm_match:
                servicio_recurso = alarm_match.group(1).strip()
        
        # Patrón 3: "Est-Eks *Critical cluster_failed_node_count* cluster-stack (EKS)"
        if not servicio_recurso:
            alarm_match = re.search(r'\*(?:Critical|Warning|Info)[^*]*\*\s*([^\(\n]+)\s*(?:\(([^\)]*)\))?', body)
            if alarm_match:
                servicio_base = alarm_match.group(1).strip() if alarm_match.group(1) else ''
                tipo_servicio = alarm_match.group(2).strip() if alarm_match.group(2) and len(alarm_match.groups()) > 1 else ''
                
                if tipo_servicio:
                    servicio_recurso = f"{servicio_base} ({tipo_servicio})"
                elif servicio_base:
                    servicio_recurso = servicio_base
        
        # Si no se encontró en el cuerpo, intentar extraer del asunto como respaldo
        if not servicio_recurso and subject:
            # Buscar entre comillas
            match = re.search(r'"([^"]+)"', subject)
            if match:
                servicio_recurso = match.group(1).strip()
            else:
                # Buscar después de asteriscos
                match = re.search(r'\*(?:Critical|Warning|Info)[^*]*\*\s*([^\n]+)', subject)
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
        # Asegurar que la tabla existe
        crear_tabla_si_no_existe()
        
        account_names = setup_aws()
        
        if periodo == 'custom' or periodo == 'diario':
            desde = datetime.now()
            if horas:
                desde -= timedelta(hours=horas)
            else: 
                desde -= timedelta(days=1)
                
            messages = get_emails(service, keyword, desde)
            df = analizar_mensajes(service, messages, account_names, horas)
            
            if not df.empty:
                insertar_alertas(df)
        else:
            df = obtener_alertas_por_periodo(periodo)

        resumen = pd.DataFrame()
        if not df.empty:
            try:
                resumen = (
                    df.groupby(['Id cuenta', 'Nombre cuenta', 'Metrica', 'Servicio', 'Estado'])
                    .size().reset_index(name='Cantidad')
                    .pivot_table(index=['Id cuenta', 'Nombre cuenta', 'Metrica', 'Servicio'], 
                                columns='Estado', values='Cantidad', fill_value=0)
                    .reset_index()
                )
                
                for estado in ['Critica', 'Warning', 'Informativo']:
                    if estado not in resumen.columns:
                        resumen[estado] = 0
                        
                resumen['Total'] = resumen[['Critica', 'Warning', 'Informativo']].sum(axis=1)
                resumen = resumen.sort_values('Total', ascending=False)
            except Exception as e:
                print(f"Error al crear resumen: {e}")

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
        nombre_archivo = f'Alertas_{periodo}{sufijo}.xlsx'
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
    
    service = setup_gmail()
    
    # Determinar las horas para el periodo personalizado
    horas = horas_custom if horas_custom else HORAS_CUSTOM if periodo == 'custom' else None
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