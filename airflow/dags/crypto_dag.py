import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DINÁMICA DEL PATH ---
# 1. Obtenemos la ruta de la carpeta 'dags'
dag_folder = os.path.dirname(os.path.abspath(__file__))

# 2. Creamos la ruta hacia la carpeta 'scripts'
scripts_path = os.path.join(dag_folder, 'scripts')

# 3. La añadimos al sistema para que Python pueda ver lo que hay dentro
if scripts_path not in sys.path:
    sys.path.append(scripts_path)
# ---------------------------------------

# Ahora los imports funcionarán directamente
try:
    from bronce import extract_crypto_data
    from transform_to_silver import process_to_silver
    from gold_analysis import generate_gold_to_postgres
except ImportError as e:
    raise ImportError(f"No se pudo importar el script: {e}. Ruta buscada: {scripts_path}")

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
    schedule=None, # Lo correremos manualmente por ahora
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

    gold_task = PythonOperator(
    task_id='generate_gold_analytics',
    python_callable=generate_gold_to_postgres  # Sin paréntesis aquí
    )

    # Definimos el orden
    extract_task >> transform_task >> gold_task

