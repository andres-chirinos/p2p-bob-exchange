import streamlit as st
import pandas as pd
import os
import plotly.graph_objects as go
import numpy as np

# Configuración básica del dashboard
st.set_page_config(
    page_title="Peer-to-Peer Boliviano (BOB) Exchange Data Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📈 Peer-to-Peer Boliviano (BOB) Exchange Data Dashboard")
st.markdown(
    """🔗 [Dataset de Kaggle](https://www.kaggle.com/datasets/andreschirinos/p2p-bob-exchange) [Almacenamiento RAW](https://drive.google.com/drive/u/0/folders/1q9XHo4nUIawftnjeLVfSBbEM5r9q3qjn) [Dashboard Interactivo](https://p2p-bob-exchange.streamlit.app/) [Repositorio de Github](https://github.com/andres-chirinos/p2p-bob-exchange)

"""
)

st.markdown("---")


# === FUNCIÓN DE CARGA DE DATOS ===
@st.cache_data(ttl=3600)
def load_summary_data():
    """
    Carga los datos de resumen pre-agregados desde el archivo parquet.
    Esto es mucho más rápido que procesar los datos completos.
    """
    try:
        # Obtener ruta del archivo de resumen
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_dir = os.path.dirname(current_dir)
        #añadele el descargar el archivo kaggle datasets download andreschirinos/p2p-bob-exchange --file dashboard_summary.parquet --path {data_dir} --force --quiet

        os.makedirs(os.path.join(project_dir, "data"), exist_ok=True)
        # Ruta del archivo parquet
        data_dir = os.path.join(project_dir, "data")
        os.system(f"kaggle datasets download andreschirinos/p2p-bob-exchange --file dashboard_summary.parquet --path {data_dir} --force --quiet")
        os.system(f"unzip -o {os.path.join(data_dir, 'dashboard_summary.parquet.zip')} -d {data_dir}")
        #os.system(f"kaggle datasets download andreschirinos/p2p-bob-exchange --file dashboard_summary_1h.parquet --path {data_dir} --force --quiet")
        #os.system(f"unzip -o {os.path.join(data_dir, 'dashboard_summary_1h.parquet.zip')} -d {data_dir}")



        # Intentar cargar el resumen de 1 hora primero (más ligero)
        data_path = os.path.join(project_dir, "data", "dashboard_summary_1h.parquet")

        if not os.path.exists(data_path):
            # Si no existe, intentar con el resumen completo
            data_path = os.path.join(project_dir, "data", "dashboard_summary.parquet")

        if not os.path.exists(data_path):
            st.error(f"❌ Archivo de resumen no encontrado: {data_path}")
            st.info(
                "💡 Ejecuta el notebook 02_transform.ipynb para generar el archivo de resumen"
            )
            return None

        # Cargar datos
        df = pd.read_parquet(data_path)

        # Verificar columnas esenciales
        required_cols = ["timestamp", "asset", "tradetype"]
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            st.error(f"❌ Columnas faltantes: {missing_cols}")
            return None

        return df

    except Exception as e:
        st.error(f"❌ Error cargando datos: {str(e)}")
        return None


# === CARGAR DATOS ===
with st.spinner("🔄 Cargando datos de resumen..."):
    df_summary = load_summary_data()

if df_summary is None:
    st.stop()

# Mostrar información básica
file_size = df_summary.memory_usage(deep=True).sum() / 1024**2
#st.success(
#    f"✅ Datos cargados: {df_summary.shape[0]:,} filas agregadas ({file_size:.2f} MB)"
#)

# === SIDEBAR CONTROLES ===
st.sidebar.header("🎛️ Controles")

# Filtro de frecuencia (si está disponible)
if "frequency" in df_summary.columns:
    available_freqs = sorted(df_summary["frequency"].unique())
    selected_frequency = st.sidebar.selectbox(
        "📊 Frecuencia de Datos", options=available_freqs, index=0
    )
    df_filtered = df_summary[df_summary["frequency"] == selected_frequency].copy()
else:
    df_filtered = df_summary.copy()
    selected_frequency = "1h"  # Default

# Filtro de tiempo
time_filter = st.sidebar.selectbox(
    "⏰ Rango de Tiempo",
    options=["Última semana", "Último mes", "Últimos 3 meses", "Último año", "Todo"],
    index=1,
)

# Aplicar filtro de tiempo
if time_filter != "Todo":
    now = pd.Timestamp.now()
    if time_filter == "Última semana":
        df_filtered = df_filtered[
            df_filtered["timestamp"] >= now - pd.Timedelta(weeks=1)
        ]
    elif time_filter == "Último mes":
        df_filtered = df_filtered[
            df_filtered["timestamp"] >= now - pd.Timedelta(days=30)
        ]
    elif time_filter == "Últimos 3 meses":
        df_filtered = df_filtered[
            df_filtered["timestamp"] >= now - pd.Timedelta(days=90)
        ]
    elif time_filter == "Último año":
        df_filtered = df_filtered[
            df_filtered["timestamp"] >= now - pd.Timedelta(days=365)
        ]

# Filtro de tipo de transacción
trade_types = st.sidebar.multiselect(
    "💱 Tipo de Transacción", options=["SELL", "BUY"], default=["SELL", "BUY"]
)

if trade_types:
    df_filtered = df_filtered[df_filtered["tradetype"].isin(trade_types)]

# Filtro de asset
available_assets = sorted(df_filtered["asset"].dropna().unique())
selected_asset = st.sidebar.selectbox(
    "🪙 Asset",
    options=available_assets,
    index=available_assets.index("USDT") if "USDT" in available_assets else 0,
)


df_asset = df_filtered[df_filtered["asset"] == selected_asset]

# === BOTÓN DE ACTUALIZACIÓN ===
if st.sidebar.button("🔄 Actualizar Datos", type="primary"):
    st.cache_data.clear()
    st.rerun()

# Mostrar información del resumen
with st.sidebar.expander("📊 Info del Resumen"):
    st.write(f"**Filas:** {df_summary.shape[0]:,}")
    st.write(f"**Columnas:** {df_summary.shape[1]}")
    st.write(f"**Memoria:** {file_size:.2f} MB")
    if "frequency" in df_summary.columns:
        st.write(f"**Frecuencias:** {', '.join(df_summary['frequency'].unique())}")
    st.write(f"**Rango temporal:**")
    st.write(f"  {df_summary['timestamp'].min().strftime('%Y-%m-%d')}")
    st.write(f"  → {df_summary['timestamp'].max().strftime('%Y-%m-%d')}")


# === VERIFICAR DATOS ===
if df_asset.empty:
    st.warning("⚠️ No hay datos para los filtros seleccionados")
    st.stop()

# === SEPARAR DATOS POR TIPO ===
df_sell = df_asset[df_asset["tradetype"] == "SELL"]
df_buy = df_asset[df_asset["tradetype"] == "BUY"]

# === MÉTRICAS PRINCIPALES ===
col1, col2, col3, col4 = st.columns(4)

with col1:
    if not df_sell.empty and "price_mean" in df_sell.columns:
        last_sell = df_sell.sort_values("timestamp")["price_mean"].iloc[-1]
        st.metric("💸 Precio Promedio SELL", f"{last_sell:.2f}")
    else:
        st.metric("💸 Precio Promedio SELL", "N/A")

with col2:
    if not df_buy.empty and "price_mean" in df_buy.columns:
        last_buy = df_buy.sort_values("timestamp")["price_mean"].iloc[-1]
        st.metric("💰 Precio Promedio BUY", f"{last_buy:.2f}")
    else:
        st.metric("💰 Precio Promedio BUY", "N/A")

with col3:
    if "tradablequantity_sum" in df_asset.columns:
        total_volume = df_asset["tradablequantity_sum"].sum()
        st.metric("📦 Volumen Total", f"{total_volume:,.0f}")
    else:
        st.metric("📦 Volumen Total", "N/A")

with col4:
    if "num_ads" in df_asset.columns:
        total_ads = df_asset["num_ads"].sum()
        st.metric("📢 Total Anuncios", f"{total_ads:,}")
    else:
        st.metric("📢 Total Anuncios", "N/A")

st.markdown("---")

# === GRÁFICO DE PRECIOS ===
st.subheader("📈 Evolución de Precios")

fig_price = go.Figure()

# Líneas para SELL y BUY usando datos agregados
if not df_sell.empty and "price_mean" in df_sell.columns:
    # Precio promedio con banda de confianza
    fig_price.add_trace(
        go.Scatter(
            x=df_sell["timestamp"],
            y=df_sell["price_mean"],
            mode="lines",
            name="SELL (promedio)",
            line=dict(color="red", width=2),
        )
    )

    # Banda min-max
    if "price_min" in df_sell.columns and "price_max" in df_sell.columns:
        fig_price.add_trace(
            go.Scatter(
                x=df_sell["timestamp"].tolist() + df_sell["timestamp"].tolist()[::-1],
                y=df_sell["price_max"].tolist() + df_sell["price_min"].tolist()[::-1],
                fill="toself",
                fillcolor="rgba(255,0,0,0.1)",
                line=dict(color="rgba(255,0,0,0)"),
                name="SELL (rango)",
                showlegend=True,
                hoverinfo="skip",
            )
        )

if not df_buy.empty and "price_mean" in df_buy.columns:
    fig_price.add_trace(
        go.Scatter(
            x=df_buy["timestamp"],
            y=df_buy["price_mean"],
            mode="lines",
            name="BUY (promedio)",
            line=dict(color="green", width=2),
        )
    )

    # Banda min-max
    if "price_min" in df_buy.columns and "price_max" in df_buy.columns:
        fig_price.add_trace(
            go.Scatter(
                x=df_buy["timestamp"].tolist() + df_buy["timestamp"].tolist()[::-1],
                y=df_buy["price_max"].tolist() + df_buy["price_min"].tolist()[::-1],
                fill="toself",
                fillcolor="rgba(0,255,0,0.1)",
                line=dict(color="rgba(0,255,0,0)"),
                name="BUY (rango)",
                showlegend=True,
                hoverinfo="skip",
            )
        )

fig_price.update_layout(
    title=f"Precios {selected_asset} - Frecuencia: {selected_frequency}",
    xaxis_title="Tiempo",
    yaxis_title="Precio (BOB)",
    height=500,
    showlegend=True,
    hovermode="x unified",
)

st.plotly_chart(fig_price, use_container_width=True)

# === GRÁFICO DE VOLUMEN ===
st.subheader("📊 Volumen de Transacciones")

fig_volume = go.Figure()

if not df_sell.empty and "tradablequantity_sum" in df_sell.columns:
    fig_volume.add_trace(
        go.Bar(
            x=df_sell["timestamp"],
            y=df_sell["tradablequantity_sum"],
            name="Volumen SELL",
            marker_color="red",
            opacity=0.7,
        )
    )

if not df_buy.empty and "tradablequantity_sum" in df_buy.columns:
    fig_volume.add_trace(
        go.Bar(
            x=df_buy["timestamp"],
            y=df_buy["tradablequantity_sum"],
            name="Volumen BUY",
            marker_color="green",
            opacity=0.7,
        )
    )

fig_volume.update_layout(
    title=f"Volumen Acumulado {selected_asset} - Frecuencia: {selected_frequency}",
    xaxis_title="Tiempo",
    yaxis_title="Volumen",
    height=400,
    barmode="group",
    hovermode="x unified",
)

st.plotly_chart(fig_volume, use_container_width=True)

# === GRÁFICO DE ACTIVIDAD (NÚMERO DE ANUNCIOS) ===
if "num_ads" in df_asset.columns:
    st.subheader("📢 Actividad del Mercado")

    fig_activity = go.Figure()

    if not df_sell.empty:
        fig_activity.add_trace(
            go.Scatter(
                x=df_sell["timestamp"],
                y=df_sell["num_ads"],
                mode="lines+markers",
                name="Anuncios SELL",
                line=dict(color="red", width=2),
                marker=dict(size=4),
            )
        )

    if not df_buy.empty:
        fig_activity.add_trace(
            go.Scatter(
                x=df_buy["timestamp"],
                y=df_buy["num_ads"],
                mode="lines+markers",
                name="Anuncios BUY",
                line=dict(color="green", width=2),
                marker=dict(size=4),
            )
        )

    fig_activity.update_layout(
        title=f"Número de Anuncios Activos - {selected_asset}",
        xaxis_title="Tiempo",
        yaxis_title="Cantidad de Anuncios",
        height=400,
        hovermode="x unified",
    )

    st.plotly_chart(fig_activity, use_container_width=True)

# === ESTADÍSTICAS DEL PERÍODO ===
st.markdown("---")
st.subheader("📊 Estadísticas del Período Seleccionado")

col1, col2 = st.columns(2)

with col1:
    st.write("**SELL:**")
    if not df_sell.empty and "price_mean" in df_sell.columns:
        st.write(f"- Precio promedio: {df_sell['price_mean'].mean():.2f} BOB")
        if "price_max" in df_sell.columns:
            st.write(f"- Precio máximo: {df_sell['price_max'].max():.2f} BOB")
        if "price_min" in df_sell.columns:
            st.write(f"- Precio mínimo: {df_sell['price_min'].min():.2f} BOB")
        if "tradablequantity_sum" in df_sell.columns:
            st.write(f"- Volumen total: {df_sell['tradablequantity_sum'].sum():,.0f}")
        if "num_transactions" in df_sell.columns:
            st.write(f"- Transacciones: {df_sell['num_transactions'].sum():,}")

with col2:
    st.write("**BUY:**")
    if not df_buy.empty and "price_mean" in df_buy.columns:
        st.write(f"- Precio promedio: {df_buy['price_mean'].mean():.2f} BOB")
        if "price_max" in df_buy.columns:
            st.write(f"- Precio máximo: {df_buy['price_max'].max():.2f} BOB")
        if "price_min" in df_buy.columns:
            st.write(f"- Precio mínimo: {df_buy['price_min'].min():.2f} BOB")
        if "tradablequantity_sum" in df_buy.columns:
            st.write(f"- Volumen total: {df_buy['tradablequantity_sum'].sum():,.0f}")
        if "num_transactions" in df_buy.columns:
            st.write(f"- Transacciones: {df_buy['num_transactions'].sum():,}")

st.markdown("---")
# === INFORMACIÓN ADICIONAL ===
with st.expander("ℹ️ Acerca de este Dashboard"):
    st.markdown(
        """
    ### Dashboard Optimizado con Datos Agregados
    
    Este dashboard utiliza datos **pre-agregados** en lugar de los datos raw completos, lo que proporciona:
    
    - ⚡ **Carga ultra-rápida**: ~100x más rápido que procesar datos completos
    - 💾 **Menor uso de memoria**: Reducción de ~99% en tamaño de datos
    - 📊 **Misma información**: Métricas estadísticas precisas
    - 🔄 **Actualización eficiente**: Los datos se agregan en el ETL pipeline
    
    **Columnas del resumen:**
    - `price_mean`: Precio promedio en el intervalo
    - `price_min/max`: Rango de precios
    - `price_std`: Desviación estándar
    - `tradablequantity_sum`: Volumen total
    - `num_ads`: Número de anuncios
    - `num_transactions`: Número de transacciones
    
    **Frecuencias disponibles:** 5min, 1h, 1D
    """
    )

st.caption("Dashboard P2P BOB Exchange - Powered by Streamlit - Made by Andrés Chirinos")
