from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import sys
import os

# Añadir el directorio de scripts al path para importar extract_esios
sys.path.append('/scripts/python')
from extract_esios import fetch_energy_prices

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'energy_prices_etl',
    default_args=default_args,
    description='ETL para precios de energía en España desde ESIOS',
    schedule_interval='0 3 * * *',  # A las 3 AM todos los días
    catchup=False,
)

def extract_energy_prices(**kwargs):
    """Extrae los precios de la energía para la fecha de ejecución"""
    execution_date = kwargs['ds']  # Formato YYYY-MM-DD
    print(f"Extrayendo datos para fecha: {execution_date}")
    
    # Crear directorios si no existen
    os.makedirs('/data/raw', exist_ok=True)
    os.makedirs('/data/processed', exist_ok=True)
    
    # Realizar la extracción de datos
    df = fetch_energy_prices(execution_date)
    
    print(f"Datos extraídos: {len(df)} registros")
    return execution_date

def load_to_postgres(**kwargs):
    """Carga los datos procesados desde CSV a PostgreSQL"""
    try:
        execution_date = kwargs['ti'].xcom_pull(task_ids='extract_data')
        print(f"Fecha de ejecución recibida: {execution_date}")
        
        # Conexión a Postgres
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        
        # Crear el esquema si no existe
        pg_hook.run("CREATE SCHEMA IF NOT EXISTS energia;")
        
        # Crear tabla temporal
        create_table_sql = """
        DROP TABLE IF EXISTS energia.temp_prices;
        CREATE TABLE energia.temp_prices (
            fecha VARCHAR(10),
            hora INTEGER,
            precio_kwh NUMERIC(10,5),
            unidad VARCHAR(10)
        );
        """
        pg_hook.run(create_table_sql)
        
        # Leer el CSV
        csv_path = f"/data/processed/precios_energia_{execution_date}.csv"
        print(f"Intentando leer archivo CSV desde: {csv_path}")
        
        # Verificar si el archivo existe
        import os
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Archivo no encontrado: {csv_path}")
        
        # Leer CSV
        import pandas as pd
        df = pd.read_csv(csv_path)
        print(f"CSV leído correctamente. Registros: {len(df)}")
        
        # Insertar datos fila por fila
        insert_rows = []
        for _, row in df.iterrows():
            insert_rows.append((
                str(row['fecha']), 
                int(row['hora']), 
                float(row['precio_kwh']), 
                str(row['unidad'])
            ))
        
        # Insertar usando execute_batch para mejor rendimiento
        insert_sql = """
        INSERT INTO energia.temp_prices (fecha, hora, precio_kwh, unidad) 
        VALUES (%s, %s, %s, %s)
        """
        pg_hook.insert_rows(
            table="energia.temp_prices",
            rows=insert_rows,
            target_fields=["fecha", "hora", "precio_kwh", "unidad"]
        )
        
        print(f"Datos cargados a PostgreSQL: {len(df)} registros")
        return execution_date
    except Exception as e:
        import traceback
        print(f"Error en load_to_postgres: {e}")
        print(traceback.format_exc())
        raise

# Definir la consulta SQL para la tarea update_statistics
update_stats_sql = """
-- Cargar datos desde la tabla temporal a la tabla de precios
-- Primero, eliminamos duplicados en la tabla temporal
CREATE TEMPORARY TABLE temp_prices_deduplicated AS
SELECT DISTINCT ON (fecha, hora) fecha, hora, precio_kwh, unidad
FROM energia.temp_prices;

-- Ahora insertamos desde la tabla temporal sin duplicados
INSERT INTO energia.precios_diarios (fecha, hora, precio_kwh, unidad)
SELECT 
    TO_DATE(fecha, 'YYYY-MM-DD'),
    hora,
    precio_kwh,
    unidad
FROM 
    temp_prices_deduplicated
ON CONFLICT (fecha, hora) 
DO UPDATE SET 
    precio_kwh = EXCLUDED.precio_kwh,
    unidad = EXCLUDED.unidad;

-- Actualizar estadísticas diarias
INSERT INTO energia.estadisticas_diarias 
(fecha, precio_medio, precio_maximo, precio_minimo, hora_pico, hora_valle)
SELECT 
    TO_DATE('{{ ds }}', 'YYYY-MM-DD') as fecha,
    AVG(precio_kwh) as precio_medio,
    MAX(precio_kwh) as precio_maximo,
    MIN(precio_kwh) as precio_minimo,
    (SELECT hora FROM energia.precios_diarios pd 
     WHERE pd.fecha = TO_DATE('{{ ds }}', 'YYYY-MM-DD')
     ORDER BY precio_kwh DESC LIMIT 1) as hora_pico,
    (SELECT hora FROM energia.precios_diarios pd 
     WHERE pd.fecha = TO_DATE('{{ ds }}', 'YYYY-MM-DD')
     ORDER BY precio_kwh ASC LIMIT 1) as hora_valle
FROM 
    energia.precios_diarios p
WHERE 
    fecha = TO_DATE('{{ ds }}', 'YYYY-MM-DD')
GROUP BY 
    fecha
ON CONFLICT (fecha) 
DO UPDATE SET 
    precio_medio = EXCLUDED.precio_medio,
    precio_maximo = EXCLUDED.precio_maximo,
    precio_minimo = EXCLUDED.precio_minimo,
    hora_pico = EXCLUDED.hora_pico,
    hora_valle = EXCLUDED.hora_valle;
"""

# Tareas
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_energy_prices,
    provide_context=True,
    dag=dag,
)

load_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

# Crear la tarea para actualizar estadísticas
update_stats_task = PostgresOperator(
    task_id='update_statistics',
    postgres_conn_id='postgres_default',
    sql=update_stats_sql,
    dag=dag,
)

# Definir dependencias
extract_task >> load_postgres_task >> update_stats_task