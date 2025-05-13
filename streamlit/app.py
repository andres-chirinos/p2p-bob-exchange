import streamlit as st
import pandas as pd
import os
import plotly.graph_objects as go

# Configuración básica del dashboard
st.set_page_config(page_title="Dashboard Financiero", layout="wide")

st.title("📈 Dashboard Financiero (datos desde Kaggle)")
st.markdown(
    "Este dashboard muestra datos financieros que se actualizan periódicamente desde un dataset público en Kaggle."
)

# === PARTE 1: Cargar dataset desde Kaggle ===

@st.cache_data(ttl=300)  # guarda en caché por 5 minutos
def cargar_datos():
    # Ruta al archivo dentro del dataset descargado
    ruta_archivo = "advice.parquet"

    # Descargar el dataset (solo si no existe localmente)
    if not os.path.exists(ruta_archivo):
        st.info("Descargando dataset desde Kaggle...")
        os.system(
            "kaggle datasets download andreschirinos/p2p-bob-exchange --file advice.parquet"
        )

    # Cargar datos
    df = pd.read_parquet(ruta_archivo)
    # Se eliminan filtrados estáticos para asset, de forma que se pueda elegir en la UI
    expected_columns = {
        "adv_advno": str,
        "adv_classify": "category",
        "adv_tradetype": "category",
        "adv_asset": "category",
        "adv_fiatunit": "category",
        "adv_price": float,
        "adv_surplusamount": float,
        "adv_tradablequantity": float,
        "adv_maxsingletransamount": float,
        "adv_minsingletransamount": float,
        "adv_paytimelimit": int,
        "adv_takeradditionalkycrequired": bool,
        "adv_assetscale": int,
        "adv_fiatscale": int,
        "adv_pricescale": int,
        "adv_fiatsymbol": "category",
        "adv_istradable": bool,
        "adv_dynamicmaxsingletransamount": float,
        "adv_minsingletransquantity": float,
        "adv_maxsingletransquantity": float,
        "adv_dynamicmaxsingletransquantity": float,
        "adv_issafepayment": bool,
        "timestamp": lambda col: pd.to_datetime(col, unit="s"),
        "source": "category",
    }

    # Eliminar columnas no esperadas
    df = df[[col for col in df.columns if col in expected_columns]]

    # Se comenta la conversión de tipos para mayor flexibilidad, pero se puede reactivar si es necesario
    #for col, dtype in expected_columns.items():
    #    if col in df.columns:
    #        try:
    #            df[col] = df[col].astype(dtype)
    #        except ValueError:
    #            st.warning(f"Column {col} contains invalid values and will be coerced to NaN.")
    #            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Filtrar solo los anuncios de tipo SELL
    return df[df["adv_tradetype"] == "SELL"]

df_all = cargar_datos()

# === CONTROLES en la barra lateral ===

st.sidebar.header("Controles de Visualización")

# Selección de Asset
assets = sorted(df_all["adv_asset"].dropna().unique())
asset_selection = st.sidebar.selectbox("Seleccionar Asset", options=assets, index=assets.index("USDT") if "USDT" in assets else 0)
df_asset = df_all[df_all["adv_asset"] == asset_selection]

# Selección de frecuencia para resampleo
time_options = {
    "5 minutos": "5T",
    "15 minutos": "15T",
    "30 minutos": "30T",
    "1 hora": "1H",
    "1 dia": "1D",
    "1 semana": "1W",
    "1 mes": "1M",
    "1 año": "1Y",
    "Custom": "Custom"
}
time_choice = st.sidebar.selectbox("Intervalo de tiempo", options=list(time_options.keys()))
if time_options[time_choice] == "Custom":
    custom_freq = st.sidebar.text_input("Escribe el intervalo (alias pandas, ej. '2T' para 2 minutos)", value="5T")
    freq = custom_freq
else:
    freq = time_options[time_choice]

# === PARTE 2: Procesar datos y visualización ===

# Eliminar outliers en 'adv_price' usando el método IQR
q1 = df_asset['adv_price'].quantile(0.25)
q3 = df_asset['adv_price'].quantile(0.75)
iqr = q3 - q1
df_filtered = df_asset[(df_asset['adv_price'] >= (q1 - 1.5 * iqr)) & (df_asset['adv_price'] <= (q3 + 1.5 * iqr))]

# Agregar agrupación OHLC según el intervalo seleccionado
df_ohlc = df_filtered.set_index('timestamp').resample(freq).agg({
    'adv_price': ['mean', 'max', 'min', 'median']
})
df_ohlc.columns = ['open', 'high', 'low', 'close']
df_ohlc.dropna(inplace=True)

st.subheader("📊 Gráfico de Tiempo")

fig = go.Figure(
    data=[
        go.Candlestick(
            x=df_ohlc.index,
            open=df_ohlc['open'],
            high=df_ohlc['high'],
            low=df_ohlc['low'],
            close=df_ohlc['close'],
        )
    ]
)
fig.update_layout(title=f"Gráfico de Velas - Asset: {asset_selection}, Intervalo: {freq}")
st.plotly_chart(fig, use_container_width=True)
