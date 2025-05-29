import streamlit as st
import pandas as pd
import os
import plotly.graph_objects as go

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

# === Configuración de Kaggle API ===

# Verificar si el archivo .kaggle.json existe
kaggle_dir = os.path.expanduser("~/.kaggle")
kaggle_file = os.path.join(kaggle_dir, "kaggle.json")

# if not os.path.exists(kaggle_file):
#    st.warning(
#        "El archivo .kaggle.json no existe. Por favor, proporcione sus credenciales de Kaggle."
#    )
#    kaggle_username = st.text_input("Ingrese su nombre de usuario de Kaggle")
#    kaggle_key = st.text_input("Ingrese su clave de API de Kaggle", type="password")
#
#    if kaggle_username and kaggle_key:
#        os.makedirs(kaggle_dir, exist_ok=True)
#        with open(kaggle_file, "w") as f:
#            f.write(f'{{"username":"{kaggle_username}","key":"{kaggle_key}"}}')
#        os.chmod(kaggle_file, 0o600)  # Asegurar permisos correctos
#        st.success("Archivo .kaggle.json creado exitosamente.")

# === PARTE 1: Cargar dataset desde Kaggle ===


@st.cache_data(ttl=1800)
def cargar_datos():
    # Usar un directorio de datos accesible (p. ej. usando experimental_user_data_dir en Streamlit Cloud)
    data_dir = (
        st.experimental_user_data_dir()
        if hasattr(st, "experimental_user_data_dir")
        else os.getcwd()
    )
    os.makedirs(data_dir, exist_ok=True)
    ruta_archivo = os.path.join(data_dir, "advice.parquet")

    # Descargar el dataset (solo si no existe localmente)
    #if not os.path.exists(ruta_archivo):
        # st.info("Descargando dataset desde Kaggle...")
    comando = f"kaggle datasets download andreschirinos/p2p-bob-exchange --file advice.parquet --path {data_dir} --force --quiet"
    os.system(comando)
        # Descomprimir el archivo si es necesario (si el dataset se descarga como ZIP, por ejemplo)
        # Aquí asumir que ya descarga el parquet directamente

    if not os.path.exists(ruta_archivo):
        # st.error("No se pudo obtener el archivo advice.parquet.")
        return None

    # Cargar datos
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

    df = df[[col for col in df.columns if col in expected_columns]]
    return df


df_all = cargar_datos()
st.sidebar.header("Controles de Visualización")

# Selección de tipo de transacción (SELL, BUY o ambos)
trade_types = ["SELL", "BUY"]
trade_type_selection = st.sidebar.multiselect(
    "Seleccionar Tipo de Transacción",
    options=trade_types, 
    default=trade_types,
)
if trade_type_selection:
    df_all = df_all[df_all["tradetype"].isin(trade_type_selection)]

# Selección de Asset
assets = sorted(df_all["asset"].dropna().unique())
asset_selection = st.sidebar.selectbox(
    "Seleccionar Asset",
    options=assets,
    index=assets.index("USDT") if "USDT" in assets else 0,
)
df_asset = df_all[df_all["asset"] == asset_selection]

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

# Selección del tipo de gráfico
chart_type = st.sidebar.radio(
    "Seleccionar Tipo de Gráfico",
    options=["Velas", "Líneas"],
    index=1,
)

if time_options[time_choice] == "Custom":
    custom_freq = st.sidebar.text_input(
        "Escribe el intervalo (alias pandas, ej. '2T' para 2 minutos)", value="2min"
    )
    freq = custom_freq
else:
    freq = time_options[time_choice]

# Filtrar datos para eliminar outliers
q1 = df_asset["price"].quantile(0.25)
q3 = df_asset["price"].quantile(0.75)
iqr = q3 - q1
df_filtered = df_asset[
    (df_asset["price"] >= (q1 - 1.5 * iqr))
    & (df_asset["price"] <= (q3 + 1.5 * iqr))
]

# Separar datos para SELL y BUY
df_sell = df_filtered[df_filtered["tradetype"] == "SELL"]
df_buy = df_filtered[df_filtered["tradetype"] == "BUY"]
# Agregar agrupación OHLC para SELL según el intervalo seleccionado
df_ohlc_sell = (
    df_sell.set_index("timestamp")
    .resample(freq)
    .agg({"price": ["mean", "max", "min", "median"]})
)
df_ohlc_sell.columns = ["open", "high", "low", "close"]
df_ohlc_sell.dropna(inplace=True)

# Agregar agrupación OHLC para BUY según el intervalo seleccionado
df_ohlc_buy = (
    df_buy.set_index("timestamp")
    .resample(freq)
    .agg({"price": ["mean", "max", "min", "median"]})
)
df_ohlc_buy.columns = ["open", "high", "low", "close"]
df_ohlc_buy.dropna(inplace=True)

# Calcular los precios del último periodo para SELL y BUY
ultimo_precio_sell = df_ohlc_sell["close"].iloc[-1] if not df_ohlc_sell.empty else None
ultimo_precio_buy = df_ohlc_buy["close"].iloc[-1] if not df_ohlc_buy.empty else None

# Mostrar indicadores globales
col1, col2 = st.columns(2)
with col1:
    st.metric(
        label="Último Precio SELL",
        value=f"{ultimo_precio_sell:.2f}" if ultimo_precio_sell else "N/A",
    )
with col2:
    st.metric(
        label="Último Precio BUY",
        value=f"{ultimo_precio_buy:.2f}" if ultimo_precio_buy else "N/A",
    )

# Gráfico combinado de velas para SELL y BUY
st.subheader("📊 Gráfico de Precio")
fig_combined = go.Figure()

if chart_type == "Velas":
    # Agregar velas para SELL
    fig_combined.add_trace(
        go.Candlestick(
            x=df_ohlc_sell.index,
            open=df_ohlc_sell["open"],
            high=df_ohlc_sell["high"],
            low=df_ohlc_sell["low"],
            close=df_ohlc_sell["close"],
            name="Precio de Venta",
            increasing=dict(line=dict(width=1, color="red")),
            decreasing=dict(line=dict(width=1, color="darkred")),
        )
    )

    # Agregar velas para BUY
    fig_combined.add_trace(
        go.Candlestick(
            x=df_ohlc_buy.index,
            open=df_ohlc_buy["open"],
            high=df_ohlc_buy["high"],
            low=df_ohlc_buy["low"],
            close=df_ohlc_buy["close"],
            name="Precio de Compra",
            increasing=dict(line=dict(width=1, color="green")),
            decreasing=dict(line=dict(width=1, color="darkgreen")),
        )
    )
elif chart_type == "Líneas":
    # Agregar líneas para SELL
    fig_combined.add_trace(
        go.Scatter(
            x=df_ohlc_sell.index,
            y=df_ohlc_sell["close"],
            mode="lines",
            name="Precio de Venta",
            line=dict(color="red", width=2),
        )
    )

    # Agregar líneas para BUY
    fig_combined.add_trace(
        go.Scatter(
            x=df_ohlc_buy.index,
            y=df_ohlc_buy["close"],
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

# Calcular volumen de compra y venta con resampleo según el intervalo seleccionado
df_volume = (
    df_filtered.set_index("timestamp")
    .groupby("tradetype")["tradablequantity"]
    .resample(freq)
    .mean()
    .reset_index()
    .pivot(index="timestamp", columns="tradetype", values="tradablequantity")
    .fillna(0)
)

# Crear gráfico de barras apiladas para volumen
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
