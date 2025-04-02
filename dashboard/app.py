# streamlit/energy_dashboard.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime, timedelta
import psycopg2
from dotenv import load_dotenv
import numpy as np

# load_dotenv()  # Carga las variables desde .env

# # Configurar las variables de conexión
# POSTGRES_USER = os.environ.get('POSTGRES_USER', 'airflow')
# POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'airflow')
# POSTGRES_DB = os.environ.get('POSTGRES_DB', 'airflow')
# POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
# POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')



# Configuración de la página
st.set_page_config(
    page_title="Dashboard Ejecutivo de Precios de Energía",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"  # Para enfocarse primero en los KPIs
)

# Título del dashboard
st.title("⚡ Dashboard Ejecutivo - Precios de Energía")

# Función para crear una conexión directa con psycopg2
# Función para conectarse a la base de datos
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=st.secrets.host,
            database=st.secrets.dbname,
            user=st.secrets.user,
            password=st.secrets.password,
            port=st.secrets.port
        )
        return conn
    except Exception as e:
        st.error(f"❌ No se pudo conectar a la base de datos: {e}")
        return None


# Función para ejecutar consultas y obtener resultados como DataFrame
@st.cache_data(ttl=3600)
def execute_query(query):
    """Ejecuta una consulta SQL y devuelve los resultados como DataFrame."""
    try:
        conn = get_db_connection()
        if conn is None:
            return pd.DataFrame()
            
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error al ejecutar consulta: {e}")
        st.code(query)
        return pd.DataFrame()

# Verificar la conexión a la base de datos silenciosamente
try:
    conn = get_db_connection()
    if conn is None:
        st.error("❌ No se pudo establecer conexión a la base de datos")
        st.stop()
    conn.close()
except Exception as e:
    st.error(f"❌ Error al conectar a la base de datos: {e}")
    st.stop()

# Consulta para obtener rango de fechas disponibles
fecha_query = "SELECT MIN(fecha) as min_date, MAX(fecha) as max_date FROM precios_energia"
fecha_df = execute_query(fecha_query)

if fecha_df.empty:
    st.error("❌ No se pudieron obtener las fechas disponibles")
    st.stop()

min_date = pd.to_datetime(fecha_df['min_date'].iloc[0])
max_date = pd.to_datetime(fecha_df['max_date'].iloc[0])

# Sidebar para filtros
with st.sidebar:
    st.header("Filtros y Parámetros")
    
    # Filtro de fecha
    date_range = st.date_input(
        "Periodo de análisis",
        value=(max_date - timedelta(days=30), max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    if len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = date_range[0]
        end_date = date_range[0]
    
    # Filtros específicos para análisis ejecutivo
    price_threshold = st.slider(
        "Umbral de precio crítico (€/MWh)",
        min_value=50,
        max_value=300,
        value=150,
        step=10,
        help="Define el umbral para destacar precios elevados"
    )
    
    # Filtros de horas
    st.subheader("Análisis por franjas horarias")
    show_hour_bands = st.checkbox("Mostrar análisis por franjas", value=True)
    
    if show_hour_bands:
        hour_bands = {
            "Valle (0-7h)": (0, 7),
            "Llano (8-17h)": (8, 17),
            "Punta (18-23h)": (18, 23)
        }
    
    st.markdown("---")
    st.caption("Dashboard para decisiones ejecutivas")
    
    # Mostrar última actualización de datos
    last_update_query = "SELECT MAX(fecha) as ultima_actualizacion FROM precios_energia"
    last_update_df = execute_query(last_update_query)
    
    if not last_update_df.empty:
        last_update = pd.to_datetime(last_update_df['ultima_actualizacion'].iloc[0])
        st.info(f"Última actualización: {last_update.strftime('%d/%m/%Y')}")

# Cargar datos recientes (último mes)
recent_data_query = f"""
    SELECT fecha, hora, precio_kwh, unidad
    FROM precios_energia
    WHERE fecha BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY fecha, hora
"""

# Cargar datos del periodo anterior para comparar
previous_start = start_date - timedelta(days=(end_date - start_date).days)
previous_end = start_date - timedelta(days=1)

previous_data_query = f"""
    SELECT fecha, hora, precio_kwh, unidad
    FROM precios_energia
    WHERE fecha BETWEEN '{previous_start}' AND '{previous_end}'
    ORDER BY fecha, hora
"""

# Ejecutar consultas
recent_df = execute_query(recent_data_query)
previous_df = execute_query(previous_data_query)

if recent_df.empty:
    st.warning("No se encontraron datos para el período seleccionado")
    st.stop()

# Preparar datos
recent_df['fecha'] = pd.to_datetime(recent_df['fecha'])
recent_df['datetime'] = recent_df.apply(lambda row: row['fecha'] + timedelta(hours=int(row['hora'])), axis=1)

if not previous_df.empty:
    previous_df['fecha'] = pd.to_datetime(previous_df['fecha'])
    previous_df['datetime'] = previous_df.apply(lambda row: row['fecha'] + timedelta(hours=int(row['hora'])), axis=1)

# Calcular KPIs
current_avg = recent_df['precio_kwh'].mean()
current_max = recent_df['precio_kwh'].max()
current_min = recent_df['precio_kwh'].min()
current_std = recent_df['precio_kwh'].std()
high_price_hours = len(recent_df[recent_df['precio_kwh'] > price_threshold])
high_price_percentage = (high_price_hours / len(recent_df)) * 100

# Calcular KPIs para periodo anterior si hay datos
if not previous_df.empty:
    previous_avg = previous_df['precio_kwh'].mean()
    previous_max = previous_df['precio_kwh'].max()
    previous_min = previous_df['precio_kwh'].min()
    avg_change = ((current_avg - previous_avg) / previous_avg) * 100
    max_change = ((current_max - previous_max) / previous_max) * 100
    min_change = ((current_min - previous_min) / previous_min) * 100
else:
    avg_change = 0
    max_change = 0
    min_change = 0

# Calcular indicadores por franjas horarias
if show_hour_bands:
    band_stats = {}
    for band_name, (start_hour, end_hour) in hour_bands.items():
        band_data = recent_df[(recent_df['hora'] >= start_hour) & (recent_df['hora'] <= end_hour)]
        band_stats[band_name] = {
            'avg': band_data['precio_kwh'].mean(),
            'max': band_data['precio_kwh'].max(),
            'min': band_data['precio_kwh'].min(),
            'std': band_data['precio_kwh'].std(),
            'high_price_hours': len(band_data[band_data['precio_kwh'] > price_threshold]),
            'total_hours': len(band_data)
        }
        band_stats[band_name]['high_price_percentage'] = (band_stats[band_name]['high_price_hours'] / band_stats[band_name]['total_hours']) * 100

# Panel de KPIs
st.header("📊 Indicadores Clave de Rendimiento (KPIs)")

kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    st.metric(
        label="Precio Medio",
        value=f"{current_avg:.2f} €/MWh",
        delta=f"{avg_change:.1f}%" if not previous_df.empty else None
    )

with kpi2:
    st.metric(
        label="Precio Máximo",
        value=f"{current_max:.2f} €/MWh",
        delta=f"{max_change:.1f}%" if not previous_df.empty else None
    )

with kpi3:
    st.metric(
        label="Volatilidad (Desv. Estándar)",
        value=f"{current_std:.2f} €/MWh"
    )

with kpi4:
    st.metric(
        label=f"Horas > {price_threshold}€/MWh",
        value=f"{high_price_hours} h ({high_price_percentage:.1f}%)"
    )

# Gráfico principal: Evolución diaria con tendencia y umbral
st.header("📈 Evolución del Precio de Mercado")

# Calcular precio diario promedio
daily_avg = recent_df.groupby('fecha')['precio_kwh'].mean().reset_index()

# Crear figura con doble eje Y
fig = go.Figure()

# Añadir precios por hora
fig.add_trace(
    go.Scatter(
        x=recent_df['datetime'],
        y=recent_df['precio_kwh'],
        name='Precio Horario',
        line=dict(color='royalblue', width=1),
        opacity=0.6
    )
)

# Añadir promedio diario
fig.add_trace(
    go.Scatter(
        x=daily_avg['fecha'],
        y=daily_avg['precio_kwh'],
        name='Promedio Diario',
        line=dict(color='red', width=2),
    )
)

# Añadir línea de umbral crítico
fig.add_shape(
    type="line",
    x0=recent_df['datetime'].min(),
    y0=price_threshold,
    x1=recent_df['datetime'].max(),
    y1=price_threshold,
    line=dict(
        color="red",
        width=1,
        dash="dash",
    )
)

# Configuración del gráfico
fig.update_layout(
    title=f"Evolución del Precio ({start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')})",
    xaxis_title="Fecha",
    yaxis_title="Precio (€/MWh)",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    hovermode="x unified",
    template="plotly_white"
)

# Añadir anotación para el umbral
fig.add_annotation(
    x=recent_df['datetime'].max(),
    y=price_threshold,
    text=f"Umbral: {price_threshold} €/MWh",
    showarrow=False,
    yshift=10,
    font=dict(color="red")
)

st.plotly_chart(fig, use_container_width=True)

# Panel de análisis por franjas
if show_hour_bands:
    st.header("⏰ Análisis por Franjas Horarias")
    
    band_cols = st.columns(len(hour_bands))
    
    for i, (band_name, stats) in enumerate(band_stats.items()):
        with band_cols[i]:
            st.subheader(band_name)
            st.metric("Precio Medio", f"{stats['avg']:.2f} €/MWh")
            st.metric("Riesgo de Precio Alto", f"{stats['high_price_percentage']:.1f}%")
            
            # Mini gráfico de precio por hora para esta franja
            start_hour, end_hour = hour_bands[band_name]
            band_data = recent_df[(recent_df['hora'] >= start_hour) & (recent_df['hora'] <= end_hour)]
            
            hourly_avg = band_data.groupby('hora')['precio_kwh'].mean().reset_index()
            
            fig = px.bar(
                hourly_avg,
                x='hora',
                y='precio_kwh',
                text_auto='.1f',
                labels={'precio_kwh': 'Promedio (€/MWh)', 'hora': 'Hora'},
            )
            
            # Formatear eje X
            fig.update_layout(
                height=250,
                margin=dict(l=0, r=0, t=30, b=0),
                xaxis=dict(tickmode='array', tickvals=list(range(start_hour, end_hour+1))),
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)

# Panel de análisis estratégico
st.header("🎯 Análisis Estratégico")

# Patrones semanales (importante para comercializadoras)
weekly_col1, weekly_col2 = st.columns(2)

with weekly_col1:
    st.subheader("Patrón Semanal")
    
    # Agregar día de la semana
    recent_df['dia_semana'] = recent_df['fecha'].dt.day_name()
    recent_df['dia_semana_num'] = recent_df['fecha'].dt.dayofweek
    
    # Crear ordenamiento para días de la semana en español
    dias_orden = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dias_es = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    mapping = dict(zip(dias_orden, dias_es))
    
    recent_df['dia_semana_es'] = recent_df['dia_semana'].map(mapping)
    recent_df['dia_semana_es'] = pd.Categorical(
        recent_df['dia_semana_es'], 
        categories=dias_es, 
        ordered=True
    )
    
    # Calcular estadísticas por día de la semana
    weekday_stats = recent_df.groupby('dia_semana_es')['precio_kwh'].agg(['mean', 'std']).reset_index()
    
    # Crear gráfico
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=weekday_stats['dia_semana_es'],
        y=weekday_stats['mean'],
        name='Precio Medio',
        error_y=dict(
            type='data',
            array=weekday_stats['std'],
            visible=True
        ),
        text=weekday_stats['mean'].round(2),
        textposition='auto'
    ))
    
    fig.update_layout(
        title="Precio Medio por Día de la Semana",
        xaxis_title="Día",
        yaxis_title="Precio Medio (€/MWh)",
        template="plotly_white"
    )
    
    st.plotly_chart(fig, use_container_width=True)

with weekly_col2:
    st.subheader("Comparativa Intradía")
    
    # Obtener horas con mayor y menor precio promedio
    hourly_avg = recent_df.groupby('hora')['precio_kwh'].mean().reset_index()
    max_hour = hourly_avg.loc[hourly_avg['precio_kwh'].idxmax()]
    min_hour = hourly_avg.loc[hourly_avg['precio_kwh'].idxmin()]
    
    # Crear figura
    fig = px.line(
        hourly_avg, 
        x='hora', 
        y='precio_kwh',
        markers=True,
        labels={'hora': 'Hora del Día', 'precio_kwh': 'Precio Promedio (€/MWh)'}
    )
    
    # Resaltar hora más cara y más barata
    fig.add_scatter(
        x=[max_hour['hora']], 
        y=[max_hour['precio_kwh']],
        mode='markers',
        marker=dict(color='red', size=12, symbol='star'),
        name='Hora más cara'
    )
    
    fig.add_scatter(
        x=[min_hour['hora']], 
        y=[min_hour['precio_kwh']],
        mode='markers',
        marker=dict(color='green', size=12, symbol='star'),
        name='Hora más barata'
    )
    
    # Añadir etiquetas para hora más cara y más barata
    fig.add_annotation(
        x=max_hour['hora'],
        y=max_hour['precio_kwh'],
        text=f"{int(max_hour['hora'])}:00h<br>{max_hour['precio_kwh']:.2f}€",
        showarrow=True,
        arrowhead=1,
    )
    
    fig.add_annotation(
        x=min_hour['hora'],
        y=min_hour['precio_kwh'],
        text=f"{int(min_hour['hora'])}:00h<br>{min_hour['precio_kwh']:.2f}€",
        showarrow=True,
        arrowhead=1,
    )
    
    # Formatear eje X para mostrar horas completas
    fig.update_xaxes(tickvals=list(range(0, 24, 2)), ticktext=[f"{h:02d}:00" for h in range(0, 24, 2)])
    
    fig.update_layout(
        title="Patrón de Precios Intradía",
        template="plotly_white"
    )
    
    st.plotly_chart(fig, use_container_width=True)

# Panel de análisis de riesgo
st.header("⚠️ Análisis de Riesgo y Volatilidad")

risk_col1, risk_col2 = st.columns(2)

with risk_col1:
    st.subheader("Distribución de Precios")
    
    # Calcular percentiles importantes
    p10 = np.percentile(recent_df['precio_kwh'], 10)
    p25 = np.percentile(recent_df['precio_kwh'], 25)
    p50 = np.percentile(recent_df['precio_kwh'], 50)
    p75 = np.percentile(recent_df['precio_kwh'], 75)
    p90 = np.percentile(recent_df['precio_kwh'], 90)
    
    # Crear histograma con marcadores verticales
    fig = px.histogram(
        recent_df, 
        x='precio_kwh',
        nbins=30,
        labels={'precio_kwh': 'Precio (€/MWh)', 'count': 'Frecuencia'},
        opacity=0.8
    )
    
    # Añadir líneas verticales para los percentiles
    fig.add_vline(x=p10, line_dash="dash", line_color="green", annotation_text="P10")
    fig.add_vline(x=p50, line_dash="dash", line_color="blue", annotation_text="Mediana")
    fig.add_vline(x=p90, line_dash="dash", line_color="red", annotation_text="P90")
    
    fig.update_layout(
        title="Distribución de Precios y Percentiles",
        xaxis_title="Precio (€/MWh)",
        yaxis_title="Frecuencia (horas)",
        showlegend=False,
        template="plotly_white"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Tabla de percentiles
    percentiles_df = pd.DataFrame({
        'Percentil': ['P10', 'P25', 'Mediana (P50)', 'P75', 'P90'],
        'Valor (€/MWh)': [p10, p25, p50, p75, p90]
    })
    
    st.table(percentiles_df.set_index('Percentil'))

with risk_col2:
    st.subheader("Horas de Alto Riesgo")
    
    # Identificar horas por encima del umbral
    high_risk = recent_df[recent_df['precio_kwh'] > price_threshold].copy()
    
    if not high_risk.empty:
        # Agrupar por hora del día
        hourly_risk = high_risk.groupby('hora').size().reset_index(name='count')
        
        # Calcular el total de días para obtener porcentajes
        total_days = (end_date - start_date).days + 1
        hourly_risk['percentage'] = (hourly_risk['count'] / total_days) * 100
        
        # Crear gráfico de barras
        fig = px.bar(
            hourly_risk, 
            x='hora', 
            y='percentage',
            text=hourly_risk['percentage'].round(1).astype(str) + '%',
            labels={'hora': 'Hora del Día', 'percentage': 'Frecuencia (%)'},
            color='percentage',
            color_continuous_scale='Reds',
        )
        
        fig.update_xaxes(tickvals=list(range(0, 24, 2)), ticktext=[f"{h:02d}:00" for h in range(0, 24, 2)])
        
        fig.update_layout(
            title=f"Frecuencia de Precios > {price_threshold} €/MWh por Hora",
            xaxis_title="Hora del Día",
            yaxis_title="% de Días",
            coloraxis_showscale=False,
            template="plotly_white"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Mensaje de resumen
        worst_hour = hourly_risk.loc[hourly_risk['percentage'].idxmax()]
        
        st.info(f"⚠️ La hora {int(worst_hour['hora']):02d}:00 presenta precios superiores a {price_threshold} €/MWh en el {worst_hour['percentage']:.1f}% de los días analizados.")
    else:
        st.info(f"No se han detectado precios por encima del umbral de {price_threshold} €/MWh en el periodo analizado.")

# Panel de conclusiones y recomendaciones
st.header("🔍 Recomendaciones Estratégicas")

# Generar conclusiones automáticas basadas en los datos
conclusions = []

# Conclusión sobre tendencia
if not previous_df.empty and avg_change > 5:
    conclusions.append(f"📈 **Tendencia alcista**: Los precios medios han aumentado un {avg_change:.1f}% respecto al periodo anterior.")
elif not previous_df.empty and avg_change < -5:
    conclusions.append(f"📉 **Tendencia bajista**: Los precios medios han disminuido un {-avg_change:.1f}% respecto al periodo anterior.")

# Conclusión sobre volatilidad
volatility_percentage = (current_std / current_avg) * 100
if volatility_percentage > 30:
    conclusions.append(f"🔄 **Alta volatilidad**: Con una desviación estándar del {volatility_percentage:.1f}% sobre el precio medio, el mercado muestra una volatilidad significativa.")

# Conclusión sobre horas de alto riesgo
if high_price_percentage > 20:
    conclusions.append(f"⚠️ **Alto riesgo de precios elevados**: El {high_price_percentage:.1f}% de las horas superan el umbral de {price_threshold} €/MWh.")

# Conclusiones sobre franjas horarias
if show_hour_bands:
    max_band_avg = max(stats['avg'] for stats in band_stats.values())
    min_band_avg = min(stats['avg'] for stats in band_stats.values())
    max_band_name = [name for name, stats in band_stats.items() if stats['avg'] == max_band_avg][0]
    min_band_name = [name for name, stats in band_stats.items() if stats['avg'] == min_band_avg][0]
    
    band_diff_percentage = ((max_band_avg - min_band_avg) / min_band_avg) * 100
    
    if band_diff_percentage > 30:
        conclusions.append(f"⏰ **Alta dispersión entre franjas**: La franja {max_band_name} es un {band_diff_percentage:.1f}% más cara que {min_band_name}.")

# Si no hay conclusiones, añadir un mensaje genérico
if not conclusions:
    conclusions.append("📊 Los precios se mantienen estables, sin cambios significativos que requieran acciones inmediatas.")

# Mostrar conclusiones
for conclusion in conclusions:
    st.info(conclusion)

# Recomendaciones basadas en análisis
st.subheader("Recomendaciones para Comercializadora")

recommendations = [
    f"⚡ **Ajustar estrategia de compra**: Considerar compras en periodos valle ({min_band_name}) para optimizar costes.",
    f"📑 **Revisión de contratos**: Para clientes de alto consumo en la franja {max_band_name}, evaluar ajustes en los términos de precios.",
    "🔍 **Monitoreo continuo**: Mantener vigilancia sobre la volatilidad de precios, especialmente en las horas identificadas como críticas."
]

for rec in recommendations:
    st.markdown(rec)

# Footer con información contextual
st.caption("Datos proporcionados por ESIOS (Red Eléctrica de España). Dashboard actualizado: " + datetime.now().strftime("%d/%m/%Y %H:%M"))