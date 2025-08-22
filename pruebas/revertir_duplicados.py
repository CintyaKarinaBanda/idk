import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.db_manager import get_connection

print("🔄 Eliminando duplicados con fecha_str...")
conn = get_connection()
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM alertas")
antes = cursor.fetchone()[0]
cursor.execute("DELETE FROM alertas a1 USING alertas a2 WHERE a1.id < a2.id AND a1.cuenta_id = a2.cuenta_id AND a1.metrica = a2.metrica AND a1.servicio = a2.servicio AND a1.estado = a2.estado AND a1.fecha_str = a2.fecha_str")
cursor.execute("SELECT COUNT(*) FROM alertas")
despues = cursor.fetchone()[0]
conn.commit()
conn.close()
print(f"✅ {antes - despues} duplicados eliminados")