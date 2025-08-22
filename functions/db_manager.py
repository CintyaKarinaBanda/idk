import psycopg2
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from config import REPORT_CONFIG, DB_CONFIG

def get_connection():
    """Obtiene una conexión a la base de datos PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["HOST"],
            database=DB_CONFIG["DATABASE"],
            user=DB_CONFIG["USER"],
            password=DB_CONFIG["PASSWORD"],
            port=DB_CONFIG["PORT"]
        )
        print(f"✅ Conexión a la base de datos establecida: {DB_CONFIG['HOST']}:{DB_CONFIG['PORT']}/{DB_CONFIG['DATABASE']}")
        return conn
    except Exception as e:
        print(f"❌ Error al conectar a la base de datos: {e}")
        raise

def get_sqlalchemy_engine():
    """Obtiene un motor SQLAlchemy para operaciones con pandas"""
    return create_engine(f'postgresql://{DB_CONFIG["USER"]}:{DB_CONFIG["PASSWORD"]}@{DB_CONFIG["HOST"]}:{DB_CONFIG["PORT"]}/{DB_CONFIG["DATABASE"]}')

def insertar_alertas(df):
    """Inserta las alertas del DataFrame en la base de datos, evitando duplicados"""
    if df.empty:
        print("⚠️ No hay alertas para insertar en la base de datos")
        return 0
    
    # Renombrar columnas para que coincidan con la tabla
    df_db = df.rename(columns={
        'Id cuenta': 'cuenta_id',
        'Nombre cuenta': 'cuenta_nombre',
        'Metrica': 'metrica',
        'Servicio': 'servicio',
        'Namespace': 'namespace',
        'Estado': 'estado',
        'Fecha': 'fecha_str'
    })
    
    # Obtener conexión y cursor
    conn = get_connection()
    cursor = conn.cursor()
    
    # Contador de filas insertadas
    inserted_count = 0
    
    # Insertar fila por fila verificando duplicados más estrictos
    for _, row in df_db.iterrows():
        # Verificar duplicados por cuenta, métrica, servicio, estado y fecha (más preciso)
        cursor.execute("""
        SELECT id FROM alertas 
        WHERE cuenta_id = %s AND metrica = %s AND servicio = %s AND estado = %s AND fecha_str = %s
        """, (row['cuenta_id'], row['metrica'], row['servicio'], row['estado'], row['fecha_str']))
        
        # Si no existe, insertar
        if cursor.fetchone() is None:
            try:
                cursor.execute("""
                INSERT INTO alertas (cuenta_id, cuenta_nombre, metrica, servicio, namespace, estado, fecha_str)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['cuenta_id'], row['cuenta_nombre'], row['metrica'], 
                    row['servicio'], row['namespace'], row['estado'], 
                    row['fecha_str']
                ))
                inserted_count += 1
            except Exception as e:
                print(f"⚠️ Error insertando fila: {e}")
                continue
    
    # Confirmar cambios y cerrar conexión
    conn.commit()
    conn.close()
    
    print(f"✅ {inserted_count} alertas insertadas en la base de datos (evitando duplicados)")
    return inserted_count

def actualizar_fechas_vacias():
    """Actualiza los registros que tienen fecha NULL usando fecha_str"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Verificar si hay registros con fecha NULL
    cursor.execute("SELECT COUNT(*) FROM alertas WHERE fecha IS NULL")
    null_count = cursor.fetchone()[0]
    
    if null_count > 0:
        print(f"⚠️ Se encontraron {null_count} registros con fecha NULL. Actualizando...")
        
        # Actualizar fechas usando fecha_str
        cursor.execute("""
        UPDATE alertas 
        SET fecha = TO_TIMESTAMP(fecha_str, 'Dy, DD Mon YYYY HH24:MI:SS +0000')
        WHERE fecha IS NULL AND fecha_str IS NOT NULL
        """)
        
        # Verificar cuántos registros se actualizaron
        cursor.execute("SELECT COUNT(*) FROM alertas WHERE fecha IS NULL")
        remaining_null = cursor.fetchone()[0]
        
        print(f"✅ {null_count - remaining_null} registros actualizados con éxito")
        if remaining_null > 0:
            print(f"⚠️ {remaining_null} registros siguen con fecha NULL")
    else:
        print("✅ No hay registros con fecha NULL")
    
    conn.commit()
    conn.close()

def verificar_tabla():
    """Verifica que la tabla de alertas exista y cuenta los registros"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Verificar si hay datos en la tabla
    cursor.execute("SELECT COUNT(*) FROM alertas")
    count = cursor.fetchone()[0]
    print(f"✅ La tabla de alertas contiene {count} registros")
    
    conn.commit()
    conn.close()
    return count

    
    conn.commit()
    conn.close()
    print("✅ Tabla de alertas verificada/creada correctamente")

def eliminar_duplicados():
    """Elimina registros duplicados de la tabla alertas"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Contar duplicados antes
    cursor.execute("SELECT COUNT(*) FROM alertas")
    total_antes = cursor.fetchone()[0]
    
    # Eliminar duplicados manteniendo el registro más reciente
    cursor.execute("""
    DELETE FROM alertas a1 USING alertas a2 
    WHERE a1.id < a2.id 
    AND a1.cuenta_id = a2.cuenta_id 
    AND a1.metrica = a2.metrica 
    AND a1.servicio = a2.servicio 
    AND a1.estado = a2.estado 
    AND ABS(EXTRACT(EPOCH FROM (a1.fecha - a2.fecha))) < 300
    """)
    
    # Contar después
    cursor.execute("SELECT COUNT(*) FROM alertas")
    total_despues = cursor.fetchone()[0]
    
    eliminados = total_antes - total_despues
    
    conn.commit()
    conn.close()
    
    print(f"✅ Duplicados eliminados: {eliminados}")
    print(f"📊 Registros antes: {total_antes}, después: {total_despues}")
    return eliminados

def obtener_alertas_por_periodo(periodo, horas=None):
    """Obtiene alertas de la base de datos según el periodo especificado"""
    engine = get_sqlalchemy_engine()
    
    # Obtener todos los registros sin filtrar por fecha
    query = """
    SELECT 
        cuenta_id as "Id cuenta",
        cuenta_nombre as "Nombre cuenta",
        metrica as "Metrica",
        servicio as "Servicio",
        namespace as "Namespace",
        estado as "Estado",
        fecha_str as "Fecha"
    FROM alertas
    """
    
    df = pd.read_sql_query(query, engine)
    
    # Agregar una fecha actual a todos los registros para que funcione el reporte
    if 'Fecha' not in df.columns or df['Fecha'].isna().all():
        print("ℹ️ Agregando fechas actuales a los registros para el reporte")
        df['Fecha'] = datetime.now()
    
    df = pd.read_sql_query(query, engine)
    
    print(f"✅ {len(df)} alertas recuperadas de la base de datos")
    print(f"  Periodo: {periodo} {f'({horas} horas)' if horas else ''}")
    return df