from functions.gmail_manager import obtener_mensajes
from datetime import datetime

hoy = datetime.now()
inicio_mes = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
fin_mes = hoy.replace(day=1, month=hoy.month+1 if hoy.month < 12 else 1, year=hoy.year+1 if hoy.month == 12 else hoy.year, hour=0, minute=0, second=0, microsecond=0)

mensajes = obtener_mensajes(inicio_mes, fin_mes)
print(f"📧 {len(mensajes)} correos con EST en {hoy.strftime('%B %Y')}")