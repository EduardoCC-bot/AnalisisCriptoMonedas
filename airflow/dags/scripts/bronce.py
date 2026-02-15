import os
import requests
import json

def extract_crypto_data():
    # Definimos la ruta
    directory = 'data/bronze'
    file_path = os.path.join(directory, 'crypto_raw.json')

    # --- ESTO SOLUCIONA EL ERROR ---
    # Crea la carpeta 'data/bronze' si no existe (parents=True crea toda la ruta)
    os.makedirs(directory, exist_ok=True)
    # -------------------------------

    # Simulación de tu lógica de extracción
    # En dags/scripts/bronce.py
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,tether&vs_currencies=usd&include_market_cap=true"
    response = requests.get(url)
    data = response.json()

    with open(file_path, 'w') as f:
        json.dump(data, f)
    
    print(f"Archivo guardado exitosamente en {file_path}")

if __name__ == "__main__":
    extract_crypto_data()