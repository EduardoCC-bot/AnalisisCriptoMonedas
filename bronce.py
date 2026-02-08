import requests
import json
from datetime import datetime

def extract_crypto_data():
    # Buscamos el precio de Bitcoin, Ethereum y Solana
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true"
    
    response = requests.get(url)
    data = response.json()
    
    # Añadimos una marca de tiempo (metadata crucial para un DE)
    data['extraction_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Guardamos en nuestra carpeta "Bronze" (Simulando un Data Lake)
    with open('data/bronze/crypto_raw.json', 'w') as f:
        json.dump(data, f)
   
    print("¡Datos extraídos con éxito a la capa Bronze!")

extract_crypto_data()

