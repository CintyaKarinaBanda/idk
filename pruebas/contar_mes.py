import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.gmail_manager import setup_gmail, get_emails
from datetime import datetime

hoy = datetime.now()
inicio_mes = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

print(f"🗓️ Buscando desde: {inicio_mes.strftime('%d/%m/%Y %H:%M')}")
print(f"🗓️ Hasta hoy: {hoy.strftime('%d/%m/%Y %H:%M')}")

service = setup_gmail()
mensajes = get_emails(service, "EST", inicio_mes, hoy)
print(f"📧 {len(mensajes)} correos con EST en {hoy.strftime('%B %Y')}")