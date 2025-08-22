import psycopg2, pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from config import DB_CONFIG

def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    print(f"✅ BD: {DB_CONFIG['HOST']}:{DB_CONFIG['PORT']}")
    return conn

def get_engine():
    return create_engine(f'postgresql://{DB_CONFIG["USER"]}:{DB_CONFIG["PASSWORD"]}@{DB_CONFIG["HOST"]}:{DB_CONFIG["PORT"]}/{DB_CONFIG["DATABASE"]}')

def insertar_alertas(df):
    if df.empty: return 0
    
    df_db = df.rename(columns={'Id cuenta': 'cuenta_id', 'Nombre cuenta': 'cuenta_nombre', 'Metrica': 'metrica', 'Servicio': 'servicio', 'Namespace': 'namespace', 'Estado': 'estado', 'Fecha': 'fecha_str'})
    
    conn = get_connection()
    cursor = conn.cursor()
    inserted = 0
    
    for _, row in df_db.iterrows():
        cursor.execute("SELECT 1 FROM alertas WHERE cuenta_id=%s AND metrica=%s AND servicio=%s AND estado=%s AND fecha_str=%s", (row['cuenta_id'], row['metrica'], row['servicio'], row['estado'], row['fecha_str']))
        if not cursor.fetchone():
            try:
                cursor.execute("INSERT INTO alertas (cuenta_id, cuenta_nombre, metrica, servicio, namespace, estado, fecha_str) VALUES (%s,%s,%s,%s,%s,%s,%s)", tuple(row))
                inserted += 1
            except: continue
    
    conn.commit()
    conn.close()
    print(f"✅ {inserted} alertas insertadas")
    return inserted

def actualizar_fechas_vacias():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM alertas WHERE fecha IS NULL")
    null_count = cursor.fetchone()[0]
    if null_count > 0:
        cursor.execute("UPDATE alertas SET fecha = TO_TIMESTAMP(fecha_str, 'Dy, DD Mon YYYY HH24:MI:SS +0000') WHERE fecha IS NULL")
        print(f"✅ {null_count} fechas actualizadas")
    conn.commit()
    conn.close()

def verificar_tabla():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM alertas")
    count = cursor.fetchone()[0]
    print(f"✅ {count} registros")
    conn.close()
    return count

def eliminar_duplicados():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM alertas")
    antes = cursor.fetchone()[0]
    cursor.execute("DELETE FROM alertas a1 USING alertas a2 WHERE a1.id < a2.id AND a1.cuenta_id = a2.cuenta_id AND a1.metrica = a2.metrica AND a1.servicio = a2.servicio AND a1.estado = a2.estado")
    cursor.execute("SELECT COUNT(*) FROM alertas")
    despues = cursor.fetchone()[0]
    conn.commit()
    conn.close()
    print(f"✅ {antes - despues} duplicados eliminados")
    return antes - despues

def obtener_alertas_por_periodo(periodo, horas=None):
    df = pd.read_sql_query("SELECT cuenta_id as \"Id cuenta\", cuenta_nombre as \"Nombre cuenta\", metrica as \"Metrica\", servicio as \"Servicio\", namespace as \"Namespace\", estado as \"Estado\", fecha_str as \"Fecha\" FROM alertas", get_engine())
    print(f"✅ {len(df)} alertas de BD")
    return df