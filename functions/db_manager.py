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
        # Verificar si ya existe una alerta con la misma cuenta, métrica y fecha
        cursor.execute("""
        SELECT id FROM alertas 
        WHERE cuenta_id = %s AND metrica = %s AND fecha = %s
        """, (row['cuenta_id'], row['metrica'], row['fecha']))
        
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

def crear_tabla_si_no_existe():
    """Crea la tabla de alertas si no existe"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Crear tabla si no existe
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS alertas (
        id SERIAL PRIMARY KEY,
        cuenta_id VARCHAR(20),
        cuenta_nombre TEXT,
        metrica TEXT,
        servicio TEXT,
        namespace TEXT,
        estado VARCHAR(20),
        fecha TIMESTAMP,
        fecha_str TEXT
    )
    """)
    
    # Crear índices para mejorar rendimiento
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_alertas_fecha ON alertas (fecha)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_alertas_cuenta ON alertas (cuenta_id)")
    
    conn.commit()
    conn.close()
    print("✅ Tabla de alertas verificada/creada correctamente")

def obtener_alertas_por_periodo(periodo, horas=None):
    """Obtiene alertas de la base de datos según el periodo especificado"""
    fecha_fin = datetime.now()
    if horas is not None:
        # Si se especifican horas, usarlas independientemente del periodo
        fecha_inicio = fecha_fin - pd.Timedelta(hours=horas)
    elif periodo == 'semanal':
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
    
    print(f"✅ {len(df)} alertas recuperadas de la base de datos")
    print(f"  Periodo: {periodo} {f'({horas} horas)' if horas else ''}")
    print(f"  Rango: {fecha_inicio_str} a {fecha_fin_str}")
    return df