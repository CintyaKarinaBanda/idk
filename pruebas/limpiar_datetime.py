import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.db_manager import get_connection

print("🔄 Eliminando registros con formato datetime...")
conn = get_connection()
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM alertas")
antes = cursor.fetchone()[0]
cursor.execute("DELETE FROM alertas WHERE fecha_str ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'")
cursor.execute("SELECT COUNT(*) FROM alertas")
despues = cursor.fetchone()[0]
conn.commit()
conn.close()
print(f"✅ {antes - despues} registros datetime eliminados")
print(f"📊 BD ahora tiene {despues} alertas")