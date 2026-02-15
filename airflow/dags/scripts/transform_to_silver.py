import pandas as pd
import json
import os
from datetime import datetime

def process_to_silver():
    # 1. Rutas absolutas para el servidor (Contenedor Airflow)
    bronze_path = '/opt/airflow/data/bronze/crypto_raw.json'
    silver_dir = '/opt/airflow/data/silver'
    silver_path = os.path.join(silver_dir, 'crypto_prices.parquet')

    # Crear carpeta Silver si no existe
    os.makedirs(silver_dir, exist_ok=True)

    if not os.path.exists(bronze_path):
        raise FileNotFoundError(f"No existe el archivo Bronze en {bronze_path}")

    with open(bronze_path, 'r') as f:
        data = json.load(f) 
    
    # 2. Extraer metadata con seguridad
    # Usamos .pop(llave, default) para que no lance KeyError si no existe
    extraction_date = data.pop('extraction_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # 3. Aplanar el JSON
    rows = []
    for coin_id, metrics in data.items():
        # Verificamos que metrics sea un diccionario (evita errores si el JSON tiene estructura rara)
        if isinstance(metrics, dict):
            metrics['coin_id'] = coin_id
            metrics['extracted_at'] = extraction_date
            rows.append(metrics)
    
    if not rows:
        print("No se encontraron datos de monedas para procesar.")
        return

    df = pd.DataFrame(rows)

    # 4. Limpieza y conversión de tipos
    # Usamos errors='coerce' por si algún valor viene nulo o con texto
    if 'usd' in df.columns:
        df['usd'] = pd.to_numeric(df['usd'], errors='coerce')
    if 'usd_market_cap' in df.columns:
        df['usd_market_cap'] = pd.to_numeric(df['usd_market_cap'], errors='coerce')
    
    # 5. Guardar en formato Parquet
    df.to_parquet(silver_path, index=False)
    
    print(f"Capa Silver actualizada en {silver_path}. Se procesaron {len(df)} monedas.")

if __name__ == "__main__":
    process_to_silver()