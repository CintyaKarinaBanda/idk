from functions.gmail_manager import setup_gmail, get_emails, get_emails_by_subject_and_body
import base64
import time
from googleapiclient.errors import HttpError

def get_email_details(service, msg_id, max_retries=3):
    """Obtiene los detalles de un correo electrónico con manejo de errores"""
    retries = 0
    while retries < max_retries:
        try:
            message = service.users().messages().get(userId='me', id=msg_id).execute()
            return message
        except HttpError as error:
            if error.resp.status in [404, 403]:
                print(f"Error al obtener detalles del correo {msg_id}: {error.resp.status}")
                return {}
            retries += 1
            if retries < max_retries:
                print(f"Error al obtener detalles del correo {msg_id}, reintentando ({retries}/{max_retries})...")
                time.sleep(2)
            else:
                print(f"No se pudieron obtener detalles del correo {msg_id} después de {max_retries} intentos")
                return {}
        except Exception as e:
            retries += 1
            if retries < max_retries:
                print(f"Error al obtener detalles del correo {msg_id}: {str(e)}, reintentando ({retries}/{max_retries})...")
                time.sleep(2)
            else:
                print(f"No se pudieron obtener detalles del correo {msg_id} después de {max_retries} intentos: {str(e)}")
                return {}

def get_email_body(service, msg_id, max_retries=3):
    """Extrae el cuerpo del mensaje de un correo con manejo de errores y reintentos"""
    retries = 0
    while retries < max_retries:
        try:
            # Intentar obtener el mensaje con un timeout más corto
            message = service.users().messages().get(userId='me', id=msg_id, format='full').execute()
            payload = message.get('payload', {})
            body = ''
            
            # Función recursiva para extraer todas las partes del mensaje
            def get_text_from_parts(parts):
                text = ''
                for part in parts:
                    # Si esta parte tiene más partes, procesarlas recursivamente
                    if 'parts' in part:
                        text += get_text_from_parts(part['parts'])
                    
                    # Extraer texto de esta parte si es texto plano o HTML
                    if part.get('mimeType') in ['text/plain', 'text/html']:
                        body_data = part['body'].get('data', '')
                        if body_data:
                            text += base64.urlsafe_b64decode(body_data).decode('utf-8', errors='ignore')
                return text
            
            # Extraer el cuerpo del mensaje
            if 'parts' in payload:
                body = get_text_from_parts(payload['parts'])
            elif 'body' in payload and 'data' in payload['body']:
                body_data = payload['body']['data']
                body = base64.urlsafe_b64decode(body_data).decode('utf-8', errors='ignore')
            
            return body
        except HttpError as error:
            # Si el error es 404 (no encontrado) o 403 (prohibido), no reintentar
            if error.resp.status in [404, 403]:
                print(f"Error al obtener el correo {msg_id}: {error.resp.status}")
                return ""
            retries += 1
            if retries < max_retries:
                print(f"Error al obtener el correo {msg_id}, reintentando ({retries}/{max_retries})...")
                time.sleep(2)  # Esperar 2 segundos antes de reintentar
            else:
                print(f"No se pudo obtener el correo {msg_id} después de {max_retries} intentos")
                return ""
        except Exception as e:
            # Para cualquier otro error (como timeout), reintentar
            retries += 1
            if retries < max_retries:
                print(f"Error al procesar el correo {msg_id}: {str(e)}, reintentando ({retries}/{max_retries})...")
                time.sleep(2)  # Esperar 2 segundos antes de reintentar
            else:
                print(f"No se pudo procesar el correo {msg_id} después de {max_retries} intentos: {str(e)}")
                return ""

def main():
    # Setup Gmail service
    service = setup_gmail()
    
    # Define los criterios de búsqueda
    subject_keywords = ["EST"]  # Palabras clave en el asunto
    body_keyword = "471112691356"  # ID a buscar en el cuerpo
    desde_hora = None  # Puedes establecer esto a una fecha específica si es necesario
    
    # 1. Buscar correos con cualquiera de las palabras clave en el asunto
    all_subject_emails = []
    for keyword in subject_keywords:
        print(f"Buscando correos con '{keyword}' en el asunto...")
        emails = get_emails(service, keyword, desde_hora)
        print(f"  - Se encontraron {len(emails)} correos con '{keyword}' en el asunto")
        
        # Agregar solo los correos que no estén ya en la lista (evitar duplicados)
        for email in emails:
            if email['id'] not in [e['id'] for e in all_subject_emails]:
                all_subject_emails.append(email)
    
    total_subject_emails = len(all_subject_emails)
    print(f"\nTotal de correos únicos con alguna de las palabras clave en el asunto: {total_subject_emails}")
    
    # 2. Ahora, contar cuántos de esos correos contienen el ID en el cuerpo
    print(f"\nVerificando cuántos de estos correos contienen '{body_keyword}' en el cuerpo...")
    print("(Este proceso puede tardar varios minutos si hay muchos correos)")
    
    matching_emails = []
    total_emails = len(all_subject_emails)
    start_index = 0  # Para poder reanudar desde un punto específico si es necesario
    
    try:
        # Mostrar progreso cada 10 correos procesados
        for i, email in enumerate(all_subject_emails[start_index:], start=start_index):
            if i % 10 == 0 and i > 0:
                print(f"Progreso: {i}/{total_emails} correos procesados ({i/total_emails*100:.1f}%)")
                
            msg_id = email['id']
            body = get_email_body(service, msg_id)
            
            # Buscar el ID en el cuerpo del correo (considerando posibles variaciones)
            if body_keyword in body:
                matching_emails.append(email)
                print(f"¡Encontrado! Correo con ID: {msg_id} contiene el ID buscado")
            # Buscar sin espacios (por si el ID está formateado diferente)
            elif body_keyword.replace(" ", "") in body.replace(" ", ""):
                matching_emails.append(email)
                print(f"¡Encontrado! Correo con ID: {msg_id} contiene el ID buscado (sin espacios)")
            # Buscar con guiones (por si el ID está formateado con guiones)
            elif body_keyword[:3] + "-" + body_keyword[3:6] + "-" + body_keyword[6:] in body:
                matching_emails.append(email)
                print(f"¡Encontrado! Correo con ID: {msg_id} contiene el ID buscado (con guiones)")
    except KeyboardInterrupt:
        # Permitir al usuario interrumpir el proceso y guardar el progreso
        print(f"\nProceso interrumpido por el usuario en el correo {i}/{total_emails}")
        print(f"Se han encontrado {len(matching_emails)} correos hasta ahora")
    except Exception as e:
        # Capturar cualquier otro error
        print(f"\nError inesperado en el correo {i}/{total_emails}: {str(e)}")
        print(f"Se han encontrado {len(matching_emails)} correos hasta ahora")
    
    print(f"Total de correos que contienen '{body_keyword}' en el cuerpo: {len(matching_emails)}")
    
    # 3. Mostrar detalles de los correos que cumplen ambos criterios
    if matching_emails:
        print("\nDetalles de los correos que cumplen ambos criterios:")
        
        # Lista para guardar los detalles de los correos
        email_details = []
        
        for i, email in enumerate(matching_emails, 1):
            try:
                msg_id = email['id']
                msg_details = get_email_details(service, msg_id)
                
                # Obtener el asunto del correo
                subject = ''
                date = ''
                sender = ''
                
                if msg_details and 'payload' in msg_details and 'headers' in msg_details['payload']:
                    for header in msg_details['payload']['headers']:
                        if header['name'].lower() == 'subject':
                            subject = header['value']
                        elif header['name'].lower() == 'date':
                            date = header['value']
                        elif header['name'].lower() == 'from':
                            sender = header['value']
                
                print(f"\n{i}. ID: {msg_id}")
                print(f"   Asunto: {subject}")
                print(f"   Fecha: {date}")
                print(f"   Remitente: {sender}")
                
                # Guardar detalles para el archivo
                email_details.append({
                    'id': msg_id,
                    'subject': subject,
                    'date': date,
                    'sender': sender
                })
                
            except Exception as e:
                print(f"\n{i}. Error al mostrar detalles del correo: {str(e)}")
        
        # Preguntar si se desea guardar los resultados en un archivo
        save_option = input("\n¿Deseas guardar los resultados en un archivo? (s/n): ")
        if save_option.lower() == 's':
            try:
                import csv
                from datetime import datetime
                
                # Crear nombre de archivo con fecha y hora
                filename = f"resultados_busqueda_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                
                with open(filename, 'w', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    # Escribir encabezados
                    writer.writerow(['ID', 'Asunto', 'Fecha', 'Remitente'])
                    # Escribir datos
                    for email in email_details:
                        writer.writerow([email['id'], email['subject'], email['date'], email['sender']])
                
                print(f"\nResultados guardados en el archivo: {filename}")
            except Exception as e:
                print(f"\nError al guardar el archivo: {str(e)}")
    else:
        print("\nNo se encontraron correos que cumplan ambos criterios.")

if __name__ == "__main__":
    main()