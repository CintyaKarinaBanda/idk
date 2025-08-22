import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.gmail_manager import setup_gmail, get_emails
from functions.db_manager import obtener_alertas_por_periodo, insertar_alertas
from datetime import datetime
import pandas as pd

hoy = datetime.now()
inicio_mes = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

print(f"🗓️ Buscando desde: {inicio_mes.strftime('%d/%m/%Y %H:%M')}")
print(f"🗓️ Hasta hoy: {hoy.strftime('%d/%m/%Y %H:%M')}")

# Gmail
service = setup_gmail()
mensajes = get_emails(service, "EST", inicio_mes, hoy)
print(f"📧 {len(mensajes)} correos Gmail")

# BD
df_bd = obtener_alertas_por_periodo("mensual")
print(f"🗄️ {len(df_bd)} alertas en BD")

# Procesar correos faltantes
sys.path.append('..')
from main import analizar_mensajes, setup_aws

cuentas_aws = setup_aws()
df_nuevas = analizar_mensajes(service, mensajes, cuentas_aws, inicio_mes, hoy)
insertadas = insertar_alertas(df_nuevas)

print(f"✅ {insertadas} alertas nuevas insertadas")