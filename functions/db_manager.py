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

def insertar_alertas(df):
    """Inserta las alertas del DataFrame en la base de datos"""
    if df.empty:
        print("⚠️ No hay alertas para insertar en la base de datos")
        return 0
    
    # Renombrar columnas para que coincidan con la tabla
    df_db = df.rename(columns={
        'Id cuenta': 'cuenta_id',
        'Nombre cuenta': 'cuenta_nombre',
        'Metrica': 'metrica',
        'Namespace': 'namespace',
        'Estado': 'estado',
        'Fecha': 'fecha',
        'Fecha_str': 'fecha_str'
    })
    
    # Insertar datos usando SQLAlchemy
    engine = get_sqlalchemy_engine()
    df_db.to_sql('alertas', engine, if_exists='append', index=False)
    
    # Obtener el número de filas insertadas
    count = len(df_db)
    
    print(f"✅ {count} alertas insertadas en la base de datos")
    return count

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