#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import json

def get_esios_token():
    """Obtener el token de autenticación desde variables de entorno"""
    token = os.environ.get('ESIOS_TOKEN')
    if not token:
        raise ValueError("ESIOS_TOKEN no está configurado en las variables de entorno")
    return token

def fetch_energy_prices(date_str=None):
    """
    Extrae los precios de la energía de la API de ESIOS para una fecha específica
    Si no se proporciona fecha, se usará la fecha actual
    
    Args:
        date_str (str, optional): Fecha en formato YYYY-MM-DD. Default: None (hoy)
        
    Returns:
        pandas.DataFrame: DataFrame con los precios por hora
    """
    # Si no se proporciona fecha, usar el día actual
    if not date_str:
        date_obj = datetime.now()
        date_str = date_obj.strftime('%Y-%m-%d')
    else:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    
    # Configurar la petición a la API
    token = get_esios_token()
    headers = {
        'Accept': 'application/json; application/vnd.esios-api-v1+json',
        'Content-Type': 'application/json',
        'x-api-key': token
    }
    
    # Indicador 1001 corresponde al precio del mercado diario (PVPC)
    url = f"https://api.esios.ree.es/indicators/1001"
    
    # Parámetros de la petición (rango de fechas)
    start_date = date_obj.strftime('%Y-%m-%dT00:00:00')
    end_date = (date_obj + timedelta(days=1)).strftime('%Y-%m-%dT00:00:00')
    
    params = {
        'start_date': start_date,
        'end_date': end_date
    }
    
    # Realizar la petición
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        raise Exception(f"Error al obtener datos: {response.status_code} - {response.text}")
    
    # Procesar la respuesta
    data = response.json()
    
    # Guardar respuesta cruda para debug
    with open(f"/data/raw/esios_response_{date_str}.json", "w") as f:
        json.dump(data, f)
    
    # Extraer valores por hora
    values = data['indicator']['values']
    
    # Crear DataFrame
    df = pd.DataFrame(values)
    
    # Convertir datetime a solo fecha y hora
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['fecha'] = df['datetime'].dt.date
    df['hora'] = df['datetime'].dt.hour
    
    # Renombrar columnas y seleccionar las necesarias
    df = df.rename(columns={'value': 'precio_kwh'})
    df['unidad'] = 'EUR/MWh'  # Unidad estándar en ESIOS
    
    # Seleccionar y ordenar columnas
    result_df = df[['fecha', 'hora', 'precio_kwh', 'unidad']].copy()
    
    # Guardar CSV procesado
    result_df.to_csv(f"/data/processed/precios_energia_{date_str}.csv", index=False)
    
    return result_df

if __name__ == "__main__":
    # Para pruebas locales
    import sys
    date_str = sys.argv[1] if len(sys.argv) > 1 else None
    df = fetch_energy_prices(date_str)
    print(df.head())