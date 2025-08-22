import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.gmail_manager import setup_gmail, get_emails
from functions.aws_manager import setup_aws
from functions.db_manager import obtener_alertas_por_periodo, insertar_alertas
from main import analizar_mensajes
from datetime import datetime

hoy = datetime.now()
inicio_mes = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

print(f"🗓️ Buscando desde: {inicio_mes.strftime('%d/%m/%Y %H:%M')}")
print(f"🗓️ Hasta hoy: {hoy.strftime('%d/%m/%Y %H:%M')}")

# Gmail con manejo de errores como main.py
try:
    service = setup_gmail()
    mensajes = get_emails(service, "EST", inicio_mes, hoy)
    print(f"📧 {len(mensajes)} correos Gmail")
except:
    service = None
    mensajes = []
    print("⚠️ Gmail no disponible")

# AWS
try:
    cuentas_aws = setup_aws()
except:
    cuentas_aws = {}

# BD
df_bd = obtener_alertas_por_periodo("mensual")
print(f"🗄️ {len(df_bd)} alertas en BD")

# Procesar y sincronizar
if service and mensajes:
    df_nuevas = analizar_mensajes(service, mensajes, cuentas_aws)
    insertadas = insertar_alertas(df_nuevas)
    print(f"✅ {insertadas} alertas nuevas insertadas")
else:
    print("❌ No se pueden procesar correos sin Gmail")