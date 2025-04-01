# dags/initial_load_energy_prices_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys
import os
sys.path.append('/scripts/python')


# Añadir la raíz del proyecto al path para poder importar módulos
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importar la función main del script de ETL
from extract_historical_data import main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'initial_load_energy_prices',
    default_args=default_args,
    description='Carga inicial de datos históricos de precios de energía',
    schedule_interval=None,  # Este DAG no se ejecuta automáticamente, se debe activar manualmente una vez
    catchup=False
)

def run_initial_load():
    """Ejecuta la carga inicial de datos históricos"""
    main('initial')

initial_load_task = PythonOperator(
    task_id='run_initial_load',
    python_callable=run_initial_load,
    dag=dag
)