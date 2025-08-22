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

# Solo contar, no procesar para evitar cambio de formato de fecha
print(f"📊 Comparación: {len(mensajes)} correos Gmail vs {len(df_bd)} alertas BD")
if len(mensajes) > len(df_bd):
    print(f"⚠️ Faltan {len(mensajes) - len(df_bd)} alertas en BD")
elif len(mensajes) < len(df_bd):
    print(f"ℹ️ BD tiene {len(df_bd) - len(mensajes)} alertas más que Gmail")
else:
    print("✅ Gmail y BD están sincronizados")