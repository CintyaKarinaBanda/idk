import psycopg2
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from config import REPORT_CONFIG, DB_CONFIG

def get_connection():
    """Obtiene una conexión a la base de datos PostgreSQL"""
    return psycopg2.connect(
        host=DB_CONFIG["HOST"],
        database=DB_CONFIG["DATABASE"],
        user=DB_CONFIG["USER"],
        password=DB_CONFIG["PASSWORD"],
        port=DB_CONFIG["PORT"]
    )

def get_sqlalchemy_engine():
    """Obtiene un motor SQLAlchemy para operaciones con pandas"""
    return create_engine(f'postgresql://{DB_CONFIG["USER"]}:{DB_CONFIG["PASSWORD"]}@{DB_CONFIG["HOST"]}:{DB_CONFIG["PORT"]}/{DB_CONFIG["DATABASE"]}')

def crear_tabla_si_no_existe():
    """Crea la tabla de alertas si no existe"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Crear tabla de alertas
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS alertas (
        id SERIAL PRIMARY KEY,
        cuenta_id TEXT,
        cuenta_nombre TEXT,
        metrica TEXT,
        servicio TEXT,
        namespace TEXT,
        estado TEXT,
        fecha TIMESTAMP,
        fecha_str TEXT
    )
    """)
    
    # Crear índice único para evitar duplicados
    try:
        cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_alertas_unique 
        ON alertas (cuenta_id, metrica, servicio, fecha)
        """)
    except Exception as e:
        print(f"Nota: No se pudo crear el índice único: {e}")
        # Continuar aunque no se pueda crear el índice
    
    conn.commit()
    conn.close()
    print("✅ Base de datos verificada/creada")

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
        'Fecha': 'fecha',
        'Fecha_str': 'fecha_str'
    })
    
    # Obtener conexión y cursor
    conn = get_connection()
    cursor = conn.cursor()
    
    # Contador de filas insertadas
    inserted_count = 0
    
    # Insertar fila por fila verificando duplicados
    for _, row in df_db.iterrows():
        # Verificar si ya existe una alerta con la misma cuenta, métrica, servicio y fecha
        cursor.execute("""
        SELECT id FROM alertas 
        WHERE cuenta_id = %s AND metrica = %s AND servicio = %s AND fecha = %s
        """, (row['cuenta_id'], row['metrica'], row['servicio'], row['fecha']))
        
        # Si no existe, insertar
        if cursor.fetchone() is None:
            cursor.execute("""
            INSERT INTO alertas (cuenta_id, cuenta_nombre, metrica, servicio, namespace, estado, fecha, fecha_str)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['cuenta_id'], row['cuenta_nombre'], row['metrica'], 
                row['servicio'], row['namespace'], row['estado'], 
                row['fecha'], row['fecha_str']
            ))
            inserted_count += 1
    
    # Confirmar cambios y cerrar conexión
    conn.commit()
    conn.close()
    
    print(f"✅ {inserted_count} alertas insertadas en la base de datos (evitando duplicados)")
    return inserted_count

def obtener_alertas_por_periodo(periodo):
    """Obtiene alertas de la base de datos según el periodo especificado"""
    fecha_fin = datetime.now()
    if periodo == 'semanal':
        fecha_inicio = fecha_fin - pd.Timedelta(days=7)
    elif periodo == 'mensual':
        fecha_inicio = fecha_fin - pd.Timedelta(days=30)
    else:  # diario
        fecha_inicio = fecha_fin - pd.Timedelta(days=1)
    
    fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d %H:%M:%S')
    fecha_fin_str = fecha_fin.strftime('%Y-%m-%d %H:%M:%S')
    
    engine = get_sqlalchemy_engine()
    query = f"""
    SELECT 
        cuenta_id as "Id cuenta",
        cuenta_nombre as "Nombre cuenta",
        metrica as "Metrica",
        servicio as "Servicio",
        namespace as "Namespace",
        estado as "Estado",
        fecha as "Fecha",
        fecha_str as "Fecha_str"
    FROM alertas 
    WHERE fecha BETWEEN '{fecha_inicio_str}' AND '{fecha_fin_str}'
    """
    
    df = pd.read_sql_query(query, engine)
    
    print(f"✅ {len(df)} alertas recuperadas de la base de datos para el periodo {periodo}")
    return df