# dags/daily_update_energy_prices_dag.py
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import sys
import os
sys.path.append('/scripts/python')

# Importar la función main del script de ETL
from extract_historical_data import main

# Configurar logging
logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 30),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=1),  # Alerta si tarda más de 1 hora
}

dag = DAG(
    'daily_update_energy_prices',
    default_args=default_args,
    description='Actualización diaria de datos de precios de energía desde ESIOS',
    schedule_interval='0 8 * * *',  # Ejecutar todos los días a las 8:00 AM
    catchup=False,
    tags=['energy', 'prices', 'daily']
)

def run_daily_update(**kwargs):
    """
    Ejecuta la actualización diaria de datos de precios de energía.
    
    Esta función llama al método main del script extract_historical_data.py
    con el modo 'daily' para realizar la actualización de datos del día anterior.
    """
    try:
        logger.info("Iniciando actualización diaria de precios de energía...")
        result = main('daily')
        logger.info("Actualización diaria completada con éxito")
        return result
    except Exception as e:
        logger.error(f"Error en la actualización diaria: {str(e)}")
        raise

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

daily_update_task = PythonOperator(
    task_id='run_daily_update',
    python_callable=run_daily_update,
    provide_context=True,
    dag=dag
)

start >> daily_update_task >> end