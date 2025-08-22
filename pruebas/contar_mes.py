import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.gmail_manager import setup_gmail, get_emails
from functions.aws_manager import setup_aws
from functions.db_manager import obtener_alertas_por_periodo, insertar_alertas
from main import analizar_mensajes
from datetime import datetime

hoy = datetime.now()
inicio_mes = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

print(f"🗓️ Buscando desde: {inicio_mes.strftime('%d/%m/%Y %H:%M')}")
print(f"🗓️ Hasta hoy: {hoy.strftime('%d/%m/%Y %H:%M')}")

try:
    service = setup_gmail()
    mensajes = get_emails(service, "EST", inicio_mes, hoy)
    print(f"📧 {len(mensajes)} correos Gmail")
except:
    service = None
    mensajes = []
    print("⚠️ Gmail no disponible")

try:
    cuentas_aws = setup_aws()
except:
    cuentas_aws = {}

df_bd = obtener_alertas_por_periodo("mensual")
print(f"🗄️ {len(df_bd)} alertas en BD")

# Solo contar, no procesar para evitar cambio de formato de fecha
print(f"📊 Comparación: {len(mensajes)} correos Gmail vs {len(df_bd)} alertas BD")
if len(mensajes) > len(df_bd):
    print(f"⚠️ Faltan {len(mensajes) - len(df_bd)} alertas en BD")
elif len(mensajes) < len(df_bd):
    print(f"ℹ️ BD tiene {len(df_bd) - len(mensajes)} alertas más que Gmail")
else:
    print("✅ Gmail y BD están sincronizados")