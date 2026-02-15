import pandas as pd
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook

def generate_gold_to_postgres():
    silver_path = '/opt/airflow/data/silver/crypto_prices.parquet'
    
    if not os.path.exists(silver_path):
        raise FileNotFoundError(f"No se encontró el archivo Silver en: {silver_path}")

    df = pd.read_parquet(silver_path)

    # --- SOLUCIÓN AL KEYERROR ---
    # Imprimimos las columnas para que las veas en el log de Airflow
    print(f"Columnas detectadas en Silver: {df.columns.tolist()}")

    # Buscamos una columna que contenga 'market_cap' por si cambió el nombre
    col_cap = [c for c in df.columns if 'market_cap' in c]
    col_price = [c for c in df.columns if 'usd' == c or 'price' in c]

    if not col_cap or not col_price:
        raise KeyError(f"No se encontraron columnas de precio o market cap. Columnas: {df.columns.tolist()}")
    
    name_cap = col_cap[0]
    name_price = col_price[0]

    # 2. Lógica de Negocio usando los nombres detectados
    total_cap = df[name_cap].sum()
    df['market_share_percentage'] = (df[name_cap] / total_cap) * 100
    df['is_highest_price'] = df[name_price] == df[name_price].max()

    # 3. Seleccionar columnas finales (renombrando para la base de datos)
    gold_df = df[[
        'coin_id', 
        name_price, 
        'market_share_percentage', 
        'is_highest_price',
        'extracted_at'
    ]].rename(columns={name_price: 'price_usd'})

    # 4. Insertar en PostgreSQL
    hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    engine = hook.get_sqlalchemy_engine()
    
    gold_df.to_sql(
        'crypto_gold_metrics', 
        engine, 
        if_exists='replace', 
        index=False
    )
    print("¡Capa Gold generada e insertada en PostgreSQL exitosamente!")