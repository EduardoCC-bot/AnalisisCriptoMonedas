import pandas as pd
import json
import os

def process_to_silver():
    # 1. Leer el archivo de la capa Bronze
    bronze_path = 'data/bronze/crypto_raw.json'
    with open(bronze_path, 'r') as f:
        data = json.load(f) 
    
    # 2. Extraer la metadata y los precios
        extraction_date = data.pop('extraction_date')   

    # 3. Aplanar el JSON para que parezca una tabla
    # Queremos una fila por cada moneda
    rows = []
    for coin_id, metrics in data.items():
        metrics['coin_id'] = coin_id
        metrics['extracted_at'] = extraction_date
        rows.append(metrics)
    
    df = pd.DataFrame(rows)

    print(f"Columnas en el DataFrame: {df.columns.tolist()}")
    
    # 4. Limpieza básica: Asegurar tipos de datos
    df['usd'] = df['usd'].astype(float)
    df['usd_market_cap'] = df['usd_market_cap'].astype(float)
    
    # 5. Guardar en formato Parquet (Eficiencia nivel DE)
    silver_path = 'data/silver/crypto_prices.parquet'
    df.to_parquet(silver_path, index=False)
    
    print(f"Capa Silver actualizada. Se procesaron {len(df)} monedas.")

if __name__ == "__main__":
    process_to_silver()