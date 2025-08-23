import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.db_manager import get_engine
import pandas as pd

# Verificar estado actual de BD
query_julio_agosto = "SELECT COUNT(*) as total FROM alertas WHERE fecha_str::date >= '2025-07-01' AND fecha_str::date <= CURRENT_DATE"
query_agosto = "SELECT COUNT(*) as total FROM alertas WHERE fecha_str::date >= date_trunc('month', CURRENT_DATE) AND fecha_str::date < date_trunc('month', CURRENT_DATE) + interval '1 month'"
query_total = "SELECT COUNT(*) as total FROM alertas"

df_julio_agosto = pd.read_sql_query(query_julio_agosto, get_engine())
df_agosto = pd.read_sql_query(query_agosto, get_engine())
df_total = pd.read_sql_query(query_total, get_engine())

print(f"📊 Estado actual BD:")
print(f"  🗄️ Total alertas: {df_total['total'].iloc[0]}")
print(f"  📅 Julio-Agosto: {df_julio_agosto['total'].iloc[0]}")
print(f"  📅 Solo Agosto: {df_agosto['total'].iloc[0]}")

# Verificar últimas inserciones
query_ultimas = "SELECT fecha_str, COUNT(*) as cantidad FROM alertas WHERE fecha_str ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' GROUP BY fecha_str ORDER BY fecha_str DESC LIMIT 5"
df_ultimas = pd.read_sql_query(query_ultimas, get_engine())
if not df_ultimas.empty:
    print(f"\n⚠️ Registros con formato datetime (recientes):")
    for _, row in df_ultimas.iterrows():
        print(f"  {row['fecha_str']}: {row['cantidad']} alertas")
else:
    print(f"\n✅ No hay registros con formato datetime")