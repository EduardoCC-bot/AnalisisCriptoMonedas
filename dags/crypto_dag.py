from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Añadimos el path para que Airflow encuentre tus scripts
sys.path.append('/opt/airflow')
from bronce import extract_crypto_data
from transform_to_silver import process_to_silver

default_args = {
    'owner': 'Eduardo Carreno',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'crypto_pipeline_dag',
    default_args=default_args,
    description='Pipeline que extrae y limpia datos de Crypto',
    schedule_interval=None, # Lo correremos manualmente por ahora
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_from_api',
        python_callable=extract_crypto_data
    )

    transform_task = PythonOperator(
        task_id='transform_to_parquet',
        python_callable=process_to_silver
    )

    # Definimos el orden: Primero extrae, luego transforma
    extract_task >> transform_task