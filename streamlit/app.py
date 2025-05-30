import streamlit as st
import pandas as pd
import os
import gc
import plotly.graph_objects as go
import numpy as np
from scipy.stats import beta as beta_dist

# Configuración básica del dashboard
st.set_page_config(
    page_title="Peer-to-Peer Boliviano (BOB) Exchange Data Dashboard", layout="wide"
)

st.title("📈 Peer-to-Peer Boliviano (BOB) Exchange Data Dashboard")
st.markdown(
    """# Peer-to-Peer Boliviano (BOB) Exchange Data
_Github Actions ETL Pipeline_

This project contains the ETL pipeline for the Peer-to-Peer Boliviano (BOB) Exchange Data. The data is collected from various sources and transformed into a clean format for analysis. The pipeline includes data extraction, transformation, and loading processes, along with data quality checks.

[Dataset de Kaggle](https://www.kaggle.com/datasets/andreschirinos/p2p-bob-exchange)
[Almacenamiento RAW](https://drive.google.com/drive/u/0/folders/1q9XHo4nUIawftnjeLVfSBbEM5r9q3qjn)
[Dashboard Interactivo](https://p2p-bob-exchange.streamlit.app/)
[Repositorio de Github](https://github.com/andres-chirinos/p2p-bob-exchange)
"""
)


# === PARTE 1: Cargar dataset desde Kaggle ===
@st.cache_data(ttl=1800)
def cargar_datos():
    # Definir directorio de datos (usando experimental_user_data_dir en Streamlit Cloud o cwd)
    data_dir = (
        st.experimental_user_data_dir()
        if hasattr(st, "experimental_user_data_dir")
        else os.getcwd()
    )
    os.makedirs(data_dir, exist_ok=True)
    ruta_archivo = os.path.join(data_dir, "advice.parquet")
    if not os.path.exists(ruta_archivo):
        # Descargar el dataset (se fuerza la descarga con --force y --quiet)
        comando = f"kaggle datasets download andreschirinos/p2p-bob-exchange --file advice.parquet --path {data_dir} --force --quiet"
        os.system(comando)

    if not os.path.exists(ruta_archivo):
        st.error("No se pudo obtener el archivo advice.parquet.")
        return None

    df = pd.read_parquet(ruta_archivo)
    expected_columns = {
        "advno": str,
        "classify": "category",
        "tradetype": "category",
        "asset": "category",
        "fiatunit": "category",
        "price": float,
        "surplusamount": float,
        "tradablequantity": float,
        "maxsingletransamount": float,
        "minsingletransamount": float,
        "paytimelimit": int,
        "takeradditionalkycrequired": bool,
        "assetscale": int,
        "fiatscale": int,
        "pricescale": int,
        "fiatsymbol": "category",
        "istradable": bool,
        "dynamicmaxsingletransamount": float,
        "minsingletransquantity": float,
        "maxsingletransquantity": float,
        "dynamicmaxsingletransquantity": float,
        "issafepayment": bool,
        "timestamp": lambda col: pd.to_datetime(col, unit="s"),
        "source": "category",
    }
    # Filtrar únicamente las columnas que se esperan
    df = df[[col for col in df.columns if col in expected_columns]]
    return df


df_all = cargar_datos()
if df_all is None:
    st.stop()

st.sidebar.header("Controles de Visualización")

# === Filtro por rango de tiempo ===
time_range = st.sidebar.radio(
    "Filtrar por rango de tiempo",
    ["Último día", "Última semana", "Último año", "Todo el tiempo"],
    index=1,
)

if time_range != "Todo el tiempo":
    now = pd.Timestamp.now()
    if time_range == "Último día":
        df_all = df_all[df_all["timestamp"] >= now - pd.Timedelta(days=1)]
    elif time_range == "Última semana":
        df_all = df_all[df_all["timestamp"] >= now - pd.Timedelta(weeks=1)]
    elif time_range == "Último año":
        df_all = df_all[df_all["timestamp"] >= now - pd.Timedelta(days=365)]

# Filtro por Tipo de Transacción (SELL, BUY o ambos)
trade_types = ["SELL", "BUY"]
trade_type_selection = st.sidebar.multiselect(
    "Seleccionar Tipo de Transacción",
    options=trade_types,
    default=trade_types,
)
if trade_type_selection:
    df_all = df_all[df_all["tradetype"].isin(trade_type_selection)]

# Selección de Asset y cálculo de df_asset
assets = sorted(df_all["asset"].dropna().unique())
asset_selection = st.sidebar.selectbox(
    "Seleccionar Asset",
    options=assets,
    index=assets.index("USDT") if "USDT" in assets else 0,
)
df_asset = df_all[df_all["asset"] == asset_selection]

# Una vez calculado df_asset, eliminar el df_all para liberar RAM
del df_all
gc.collect()

# Selección de frecuencia para resampleo
time_options = {
    "5 minutos": "5min",
    "15 minutos": "15min",
    "30 minutos": "30min",
    "1 hora": "1h",
    "1 dia": "1D",
    "1 semana": "1W",
    "1 mes": "1ME",
    "1 año": "1YE",
    "Custom": "Custom",
}
time_choice = st.sidebar.selectbox(
    "Intervalo de tiempo", options=list(time_options.keys()), index=2
)

if time_options[time_choice] == "Custom":
    custom_freq = st.sidebar.text_input(
        "Escribe el intervalo (alias pandas, ej. '2T' para 2 minutos)", value="2min"
    )
    freq = custom_freq
else:
    freq = time_options[time_choice]

# Filtrar outliers en precio
q1 = df_asset["price"].quantile(0.25)
q3 = df_asset["price"].quantile(0.75)
iqr = q3 - q1
df_filtered = df_asset
#df_filtered = df_asset[
#    (df_asset["price"] >= (q1 - 1.5 * iqr)) & (df_asset["price"] <= (q3 + 1.5 * iqr))
#]
def ohlc_weighted(x, 
                  alpha_sell=5, beta_sell=1,   # Parámetros para SELL: favorecer precios altos
                  alpha_buy=1, beta_buy=5,     # Parámetros para BUY: favorecer precios bajos
                  order_type="SELL"):
    if x.empty or x["price"].dropna().empty:
        return pd.Series({
            "open": np.nan,
            "high": np.nan,
            "low": np.nan,
            "close": np.nan,
            "weighted": np.nan
        })
    # Valores reales de apertura y cierre
    open_val = x["price"].iloc[0]
    close_val = x["price"].iloc[-1]
    high_val = x["price"].max()
    low_val = x["price"].min()
    # Normalización de los precios al rango [0,1]
    if high_val != low_val:
        normalized = (x["price"] - low_val) / (high_val - low_val)
    else:
        normalized = np.ones_like(x["price"])
    # Calcular pesos usando la distribución beta
    if order_type.upper() == "SELL":
        # Para SELL, se favorecen los precios altos (normalized cerca de 1)
        weights = beta_dist.pdf(normalized, a=alpha_sell, b=beta_sell) * x["tradablequantity"]
    else:  # BUY
        # Para BUY, se favorecen los precios bajos (normalized cerca de 0)
        weights = beta_dist.pdf(normalized, a=alpha_buy, b=beta_buy) * x["tradablequantity"]
    weighted_price = (x["price"] * weights).sum() / weights.sum() if weights.sum() != 0 else np.nan
    return pd.Series({
        "open": open_val,
        "high": high_val,
        "low": low_val,
        "close": close_val,
        "weighted": weighted_price,
    })

# Ejemplo de uso en la función de resampleo:
def obtener_ohlc(df, freq, 
                 alpha_sell=1, beta_sell=5.5, 
                 alpha_buy=5.5, beta_buy=1, 
                 order_type="SELL"):
    # Se espera que 'df' contenga, además de 'price' y 'timestamp', la columna 'tradablequantity'
    resampled = (
        df.set_index("timestamp")
          .resample(freq)
          .apply(lambda x: ohlc_weighted(x, alpha_sell, beta_sell, alpha_buy, beta_buy, order_type=order_type))
    )
    return resampled.dropna()


# Separar datos para SELL y BUY
df_sell = df_filtered[df_filtered["tradetype"] == "SELL"]
df_buy = df_filtered[df_filtered["tradetype"] == "BUY"]

df_ohlc_sell = obtener_ohlc(df_sell, freq, order_type="SELL")
df_ohlc_buy = obtener_ohlc(df_buy, freq, order_type="BUY")

ultimo_precio_sell = df_ohlc_sell["weighted"].iloc[-1] if not df_ohlc_sell.empty else None
ultimo_precio_buy = df_ohlc_buy["weighted"].iloc[-1] if not df_ohlc_buy.empty else None

# Mostrar indicadores globales
col1, col2 = st.columns(2)
with col1:
    st.metric(
        label="Último Precio SELL",
        value=f"{ultimo_precio_sell:.2f}" if ultimo_precio_sell is not None else "N/A",
    )
with col2:
    st.metric(
        label="Último Precio BUY",
        value=f"{ultimo_precio_buy:.2f}" if ultimo_precio_buy is not None else "N/A",
    )

st.subheader("📊 Gráfico de Precio")
chart_type = st.sidebar.radio(
    "Seleccionar Tipo de Gráfico",
    options=["Velas", "Líneas"],
    index=1,
)

fig_combined = go.Figure()

if chart_type == "Velas":
    # Velas para SELL
    fig_combined.add_trace(
        go.Candlestick(
            x=df_ohlc_sell.index,
            open=df_ohlc_sell["open"],
            high=df_ohlc_sell["high"],
            low=df_ohlc_sell["low"],
            close=df_ohlc_sell["weighted"],
            name="Precio de Venta",
            increasing=dict(line=dict(width=1, color="red")),
            decreasing=dict(line=dict(width=1, color="darkred")),
        )
    )
    # Velas para BUY
    fig_combined.add_trace(
        go.Candlestick(
            x=df_ohlc_buy.index,
            open=df_ohlc_buy["open"],
            high=df_ohlc_buy["high"],
            low=df_ohlc_buy["low"],
            close=df_ohlc_buy["weighted"],
            name="Precio de Compra",
            increasing=dict(line=dict(width=1, color="green")),
            decreasing=dict(line=dict(width=1, color="darkgreen")),
        )
    )
elif chart_type == "Líneas":
    # Línea para SELL
    fig_combined.add_trace(
        go.Scatter(
            x=df_ohlc_sell.index,
            y=df_ohlc_sell["weighted"],
            mode="lines",
            name="Precio de Venta",
            line=dict(color="red", width=2),
        )
    )
    # Línea para BUY
    fig_combined.add_trace(
        go.Scatter(
            x=df_ohlc_buy.index,
            y=df_ohlc_buy["weighted"],
            mode="lines",
            name="Precio de Compra",
            line=dict(color="green", width=2),
        )
    )

fig_combined.update_layout(
    title=f"Gráfico de Velas (SELL y BUY) - Asset: {asset_selection}, Intervalo: {freq}",
    xaxis_title="Tiempo",
    yaxis_title="Precio",
    autosize=True,
    height=600,
    margin=dict(l=10, r=10, t=60, b=10),
)
st.plotly_chart(fig_combined, use_container_width=True)

st.subheader("📊 Gráfico de Volumen")
df_volume = (
    df_filtered.set_index("timestamp")
    .groupby("tradetype")["tradablequantity"]
    .resample(freq)
    .mean()
    .reset_index()
    .pivot(index="timestamp", columns="tradetype", values="tradablequantity")
    .fillna(0)
)

fig_volume = go.Figure()
if "BUY" in df_volume.columns:
    fig_volume.add_trace(
        go.Bar(
            x=df_volume.index,
            y=df_volume["BUY"],
            name="Volumen de Compra",
            marker_color="green",
        )
    )
if "SELL" in df_volume.columns:
    fig_volume.add_trace(
        go.Bar(
            x=df_volume.index,
            y=df_volume["SELL"],
            name="Volumen de Venta",
            marker_color="red",
        )
    )

fig_volume.update_layout(
    title=f"Volumen de Transacciones - Asset: {asset_selection}, Intervalo: {freq}",
    barmode="stack",
    xaxis_title="Tiempo",
    yaxis_title="Volumen",
    autosize=True,
    margin=dict(l=10, r=10, t=60, b=10),
)
st.plotly_chart(fig_volume, use_container_width=True)

#st.subheader("Datos Filtrados")
#st.dataframe(df_filtered)