import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Crypto Data Pipeline", layout="wide")

st.title("📈 Crypto Pipeline Dashboard (Silver Layer)")
st.write("Visualizando datos procesados desde el Data Lake en formato Parquet.")

# Función para cargar los datos
def load_data():
    df = pd.read_parquet('data/silver/crypto_prices.parquet')
    return df

try:
    df = load_data()

    # Métricas principales
    col1, col2, col3 = st.columns(3)
    for i, (index, row) in enumerate(df.iterrows()):
        cols = [col1, col2, col3]
        cols[i].metric(label=row['coin_id'].upper(), value=f"${row['usd']:,.2f}")

    # Gráfico de barras del Market Cap
    st.subheader("Capitalización de Mercado")
    fig = px.bar(df, x='coin_id', y='usd_market_cap', color='coin_id',
                 labels={'usd_market_cap': 'Market Cap (USD)', 'coin_id': 'Moneda'})
    st.plotly_chart(fig, use_container_width=True)

    st.write(f"**Última actualización del pipeline:** {df['extracted_at'].iloc[0]}")

except Exception as e:
    st.error(f"No se pudieron cargar los datos. ¿Corriste el pipeline de Docker? Error: {e}")