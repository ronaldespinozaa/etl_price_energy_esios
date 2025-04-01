import pandas as pd
import os
import numpy as np
from sqlalchemy import create_engine,text
import streamlit as st
from dotenv import load_dotenv
load_dotenv()  # Carga las variables desde .env
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'airflow')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'airflow')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'airflow')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')

# Conexión a la base de datos
@st.cache_resource
def get_db_connection():
    connection_string = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
    return create_engine(connection_string)

# Obtener datos de precios
@st.cache_data(ttl=3600)
def get_prices():
    engine = get_db_connection()
    query = """
    SELECT 
        fecha, 
        hora, 
        precio_kwh, 
        unidad 
    FROM 
        energia.precios_diarios 
    ORDER BY 
        fecha, hora
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

# Obtener estadísticas
@st.cache_data(ttl=3600)
def get_stats():
    engine = get_db_connection()
    query = """
    SELECT 
        fecha, 
        precio_medio, 
        precio_maximo, 
        precio_minimo, 
        hora_pico, 
        hora_valle 
    FROM 
        energia.estadisticas_diarias 
    ORDER BY 
        fecha
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)