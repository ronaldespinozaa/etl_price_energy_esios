#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
import sys
import json
import traceback
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
load_dotenv()  # Carga las variables desde .env

POSTGRES_USER = os.environ.get('POSTGRES_USER', 'airflow')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'airflow')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'airflow')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')

# Construir la cadena de conexión aquí
db_connection_string = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'

# Configurar logging básico
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('energy_price_etl')

# Funciones de utilidad
def create_directory(directory_path):
    """Crea un directorio si no existe."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logger.info(f"Directorio creado: {directory_path}")

def format_date(date_obj, format_str="%Y-%m-%d"):
    """Formatea un objeto datetime."""
    if isinstance(date_obj, datetime):
        return date_obj.strftime(format_str)
    return date_obj

def get_esios_token():
    """Obtener el token de autenticación desde variables de entorno"""
    token = os.getenv('ESIOS_TOKEN')
    if not token:
        raise ValueError("ESIOS_TOKEN no está configurado en las variables de entorno")
    return token

def extract_energy_prices(start_date, end_date):
    """
    Extrae precios de energía de ESIOS para un rango de fechas.
    Funciona tanto para extracción histórica como para actualizaciones diarias.
    
    Args:
        start_date (datetime): Fecha de inicio
        end_date (datetime): Fecha de fin
    
    Returns:
        pd.DataFrame: DataFrame con los precios de energía
    """
    # Configuración para la API de ESIOS
    indicator_id = 1001  # ID para precios del mercado diario
    start_date_str = format_date(start_date, "%Y-%m-%d")
    end_date_str = format_date(end_date, "%Y-%m-%d")
    token = get_esios_token()

    # URL y headers para la API de ESIOS
    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    headers = {
        "Accept": "application/json; application/vnd.esios-api-v2+json",
        "Content-Type": "application/json",
        'x-api-key': token
    }
    
    # Parámetros de la petición
    params = {
        "start_date": start_date_str + "T00:00:00",
        "end_date": end_date_str + "T23:59:59",
        "geo_ids[]": 8741  # ID para Península
    }
    
    logger.info(f"Extrayendo datos desde {start_date_str} hasta {end_date_str}")
    
    try:
        # Realizar petición a la API
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Lanzar excepción si hay error HTTP
        
        data = response.json()
        
        # Crear directorios si no existen
        raw_dir = os.path.join('data', 'raw')
        processed_dir = os.path.join('data', 'processed')
        create_directory(raw_dir)
        create_directory(processed_dir)
        
        # Guardar respuesta cruda para debug
        response_file = os.path.join(raw_dir, f"esios_response_{start_date_str}_{end_date_str}.json")
        with open(response_file, "w") as f:
            json.dump(data, f)
        logger.info(f"Respuesta de la API guardada en {response_file}")
        
        # Verificar la estructura de la respuesta
        if 'indicator' not in data or 'values' not in data['indicator'] or not data['indicator']['values']:
            logger.warning(f"La respuesta de la API no contiene datos para el período solicitado")
            return pd.DataFrame()
            
        # Extraer valores por hora
        values = data['indicator']['values']
        
        # Crear DataFrame
        df = pd.DataFrame(values)
        
        # Filtrar solo para Península (geo_id=8741)
        if 'geo_id' in df.columns:
            df = df[df['geo_id'] == 8741]
        
        # Verificar y convertir datetime a solo fecha y hora con manejo de errores
        if 'datetime' in df.columns:
            try:
                # Eliminar el offset de la zona horaria
                df['datetime'] = df['datetime'].str.split('+').str[0]

                # Convertir a datetime
                df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%dT%H:%M:%S.%f', errors='coerce')
                df = df.dropna(subset=['datetime'])  # Elimina filas con NaT
                df['fecha'] = df['datetime'].dt.date
                df['hora'] = df['datetime'].dt.hour
            except (ValueError, AttributeError) as e:
                logging.error(f"Error al convertir columna datetime: {str(e)}")
                if 'datetime' in df.columns:
                    df['fecha'] = df['datetime'].apply(lambda x: pd.to_datetime(x[:10]).date() if isinstance(x, str) else None)
                    df['hora'] = df['datetime'].apply(lambda x: int(x[11:13]) if isinstance(x, str) and len(x) > 13 else 0)
                else:
                    logging.error("Columna 'datetime' no encontrada después del error de conversión.")
                    return pd.DataFrame()
        else:
            logging.error("Columna 'datetime' no encontrada en los datos")
            return pd.DataFrame()
                
        # Renombrar columnas y seleccionar las necesarias
        if 'value' in df.columns:
            df = df.rename(columns={'value': 'precio_kwh'})
        elif 'price' in df.columns:
            df = df.rename(columns={'price': 'precio_kwh'})
        else:
            logger.error("No se encontró columna de precio en los datos")
            return pd.DataFrame()
        
        df['unidad'] = 'EUR/MWh'  # Unidad estándar en ESIOS
        
        # Seleccionar y ordenar columnas, verificando que existan
        required_columns = ['fecha', 'hora', 'precio_kwh', 'unidad']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"Columna requerida '{col}' no encontrada en los datos")
                return pd.DataFrame()
        
        result_df = df[required_columns].copy()
        
        # Guardar CSV procesado
        processed_file = os.path.join(processed_dir, f"precios_energia_{start_date_str}_{end_date_str}.csv")
        result_df.to_csv(processed_file, index=False)
        logger.info(f"Datos procesados guardados en {processed_file} ({len(result_df)} registros)")
        
        return result_df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al extraer datos: {str(e)}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error inesperado al procesar datos: {str(e)}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()

def save_to_database(df, db_connection_string, is_initial_load=False):
    """
    Guarda los datos en la base de datos.
    
    Args:
        df (pd.DataFrame): DataFrame con datos de precios
        db_connection_string (str): Cadena de conexión a la base de datos
        is_initial_load (bool): Indica si es la carga inicial de datos
    
    Returns:
        int: Número de registros insertados
    """
    if df.empty:
        logger.warning("No hay datos para guardar en la base de datos")
        return 0
    
    try:
        # Importar SQLAlchemy aquí para manejar el caso en que no esté instalado
        from sqlalchemy import create_engine
        
        # Crear conexión a partir de la cadena proporcionada
        engine = create_engine(db_connection_string)
        conn = engine.connect()
        
        # Nombre de la tabla donde guardar los datos
        table_name = 'precios_energia'
        
        # Preparar datos para la base de datos
        df_for_db = df.copy()
        df_for_db['fecha'] = pd.to_datetime(df_for_db['fecha'])
        
        if is_initial_load:
            # Si es carga inicial, primero verificamos si la tabla existe
            try:
                # Verificar si la tabla ya tiene datos
                count_query = text(f"SELECT COUNT(*) FROM {table_name}")
                result = conn.execute(count_query).scalar()
                
                if result > 0:
                    logger.info(f"La tabla {table_name} ya contiene {result} registros. Verificando duplicados...")
                    
                    # Obtener los registros existentes para evitar duplicados
                    existing_records = pd.read_sql(
                        f"SELECT fecha, hora FROM {table_name}", conn
                    )
                    
                    existing_records['fecha'] = pd.to_datetime(existing_records['fecha'])
                    
                    # Crear una clave combinada fecha-hora para comparar
                    df_for_db['key'] = df_for_db['fecha'].astype(str) + '-' + df_for_db['hora'].astype(str)
                    existing_records['key'] = existing_records['fecha'].astype(str) + '-' + existing_records['hora'].astype(str)
                    
                    # Filtrar para insertar solo los nuevos
                    df_new = df_for_db[~df_for_db['key'].isin(existing_records['key'])]
                    df_new = df_new.drop(columns=['key'])
                    
                    # Insertar nuevos registros
                    if not df_new.empty:
                        df_new.to_sql(table_name, conn, if_exists='append', index=False)
                        logger.info(f"Insertados {len(df_new)} nuevos registros en la base de datos")
                        return len(df_new)
                    else:
                        logger.info("No hay nuevos registros para insertar")
                        return 0
                else:
                    # Si la tabla existe pero está vacía, simplemente insertamos todo
                    df_for_db.to_sql(table_name, conn, if_exists='append', index=False)
                    logger.info(f"Insertados {len(df_for_db)} registros en la tabla existente")
                    return len(df_for_db)
            
            except Exception as e:
                # Si hay error (probablemente la tabla no existe), la creamos
                logger.info(f"Creando tabla {table_name}")
                df_for_db.to_sql(table_name, conn, if_exists='replace', index=False)
                logger.info(f"Creada nueva tabla {table_name} con {len(df_for_db)} registros")
                return len(df_for_db)
        else:
            # Para actualizaciones diarias, verificamos duplicados
            try:
                # Verificar si ya existen registros para las mismas fechas y horas
                date_params = ", ".join([f"'{d}'" for d in df_for_db['fecha'].dt.strftime('%Y-%m-%d').unique()])
                existing_query = f"SELECT fecha, hora FROM {table_name} WHERE fecha IN ({date_params})"
                existing_records = pd.read_sql(existing_query, conn)
                
                if not existing_records.empty:
                    existing_records['fecha'] = pd.to_datetime(existing_records['fecha'])
                    
                    # Crear una clave combinada fecha-hora para comparar
                    df_for_db['key'] = df_for_db['fecha'].astype(str) + '-' + df_for_db['hora'].astype(str)
                    existing_records['key'] = existing_records['fecha'].astype(str) + '-' + existing_records['hora'].astype(str)
                    
                    # Filtrar para insertar solo los nuevos
                    df_new = df_for_db[~df_for_db['key'].isin(existing_records['key'])]
                    df_new = df_new.drop(columns=['key'])
                else:
                    df_new = df_for_db
                
                # Insertar en la base de datos
                if not df_new.empty:
                    df_new.to_sql(table_name, conn, if_exists='append', index=False)
                    logger.info(f"Insertados {len(df_new)} nuevos registros en la base de datos")
                    return len(df_new)
                else:
                    logger.info("No hay nuevos registros para insertar")
                    return 0
            
            except Exception as e:
                logger.error(f"Error al verificar duplicados: {str(e)}")
                # Si hay error, intentamos crear la tabla
                df_for_db.to_sql(table_name, conn, if_exists='replace', index=False)
                logger.info(f"Creada nueva tabla {table_name} con {len(df_for_db)} registros")
                return len(df_for_db)
            
    except Exception as e:
        logger.error(f"Error al guardar datos en la base de datos: {str(e)}")
        logger.error(traceback.format_exc())
        return 0
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()

def check_initial_load_needed(db_connection_string):
    """
    Verifica si es necesaria una carga inicial de datos.
    
    Args:
        db_connection_string (str): Cadena de conexión a la base de datos
    
    Returns:
        bool: True si se necesita carga inicial, False en caso contrario
    """
    try:
        # Crear conexión a la base de datos
        engine = create_engine(db_connection_string)
        conn = engine.connect()
        
        # Verificar si la tabla existe y tiene datos
        try:
            count_query = text("SELECT COUNT(*) FROM precios_energia")
            result = conn.execute(count_query).scalar()
            conn.close()
            
            # Si hay menos de 100 registros, consideramos que necesita carga inicial
            if result < 100:
                logger.info(f"Se encontraron solo {result} registros. Se realizará carga inicial.")
                return True
            else:
                logger.info(f"Ya existen {result} registros en la base de datos. No es necesaria carga inicial.")
                return False
                
        except Exception as e:
            # Si hay error, probablemente la tabla no existe
            logger.info("La tabla no existe o no es accesible. Se realizará carga inicial.")
            return True
            
    except Exception as e:
        logger.error(f"Error al verificar la base de datos: {str(e)}")
        # Si no podemos verificar, asumimos que necesitamos carga inicial
        return True

def perform_initial_load(db_connection_string):
    """
    Realiza la carga inicial de datos históricos (últimos 3 años)
    
    Args:
        db_connection_string (str): Cadena de conexión a la base de datos
    
    Returns:
        bool: True si la carga fue exitosa, False en caso contrario
    """
    # Configurar fechas para los últimos 3 años
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365*3)
    
    logger.info(f"Iniciando carga inicial de datos desde {start_date.strftime('%Y-%m-%d')} "
               f"hasta {end_date.strftime('%Y-%m-%d')}")
    
    # Dividir el período en tramos de 3 meses para evitar timeouts
    all_data_frames = []
    current_start = start_date
    
    while current_start < end_date:
        # Calcular el final del tramo (3 meses después o la fecha final si es menor)
        current_end = min(current_start + timedelta(days=90), end_date)
        
        logger.info(f"Procesando tramo desde {current_start.strftime('%Y-%m-%d')} hasta {current_end.strftime('%Y-%m-%d')}")
        
        # Extraer datos para este tramo
        df_tramo = extract_energy_prices(current_start, current_end)
        
        if not df_tramo.empty:
            all_data_frames.append(df_tramo)
            logger.info(f"Tramo procesado con éxito: {len(df_tramo)} registros obtenidos")
        else:
            logger.warning(f"No se pudieron extraer datos para el tramo {current_start.strftime('%Y-%m-%d')} a {current_end.strftime('%Y-%m-%d')}")
        
        # Avanzar al siguiente tramo
        current_start = current_end + timedelta(days=1)
    
    # Combinar todos los dataframes en uno solo
    if all_data_frames:
        df_historical = pd.concat(all_data_frames, ignore_index=True)
        logger.info(f"Datos históricos combinados: total de {len(df_historical)} registros")
        
        # Guardar en la base de datos
        inserted_records = save_to_database(df_historical, db_connection_string, is_initial_load=True)
        logger.info(f"Carga inicial completada. {inserted_records} registros insertados en la base de datos.")
        
        # También guardar en archivo CSV como respaldo
        historical_dir = os.path.join('data', 'raw', 'historical')
        create_directory(historical_dir)
        
        # Crear nombre de archivo con rango de fechas
        df_historical['fecha_str'] = df_historical['fecha'].astype(str)
        min_date = min(df_historical['fecha_str']).replace('-', '')[:8]  # Formato YYYYMMDD
        max_date = max(df_historical['fecha_str']).replace('-', '')[:8]
        filename = f"energy_prices_historical_{min_date}_{max_date}.csv"
        
        # Guardar a CSV
        filepath = os.path.join(historical_dir, filename)
        df_historical.to_csv(filepath, index=False)
        
        logger.info(f"Archivo de respaldo guardado: {filepath}")
        return True
    else:
        logger.warning("No se pudieron extraer datos históricos para ningún tramo.")
        return False

def perform_daily_update(db_connection_string):
    """
    Realiza la actualización diaria de datos
    
    Args:
        db_connection_string (str): Cadena de conexión a la base de datos
    
    Returns:
        bool: True si la actualización fue exitosa, False en caso contrario
    """
    # Extraer datos del día actual y del día anterior (para asegurar completitud)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    logger.info(f"Iniciando actualización diaria para {end_date.strftime('%Y-%m-%d')}")
    
    # Extraer datos
    df_update = extract_energy_prices(start_date, end_date)
    
    if df_update.empty:
        logger.warning("No se pudieron extraer datos para la actualización diaria.")
        return False
    
    # Guardar en la base de datos
    inserted_records = save_to_database(df_update, db_connection_string, is_initial_load=False)
    logger.info(f"Actualización diaria completada. {inserted_records} registros insertados en la base de datos.")
    
    # También guardar en archivo CSV como respaldo de la actualización diaria
    updates_dir = os.path.join('data', 'updates')
    create_directory(updates_dir)
    
    # Guardar a CSV con fecha de actualización
    today_str = end_date.strftime("%Y%m%d")
    filepath = os.path.join(updates_dir, f"energy_prices_update_{today_str}.csv")
    df_update.to_csv(filepath, index=False)
    
    logger.info(f"Actualización guardada en: {filepath}")
    return True

def load_env_variables():
    """Carga variables de entorno desde archivo .env si está disponible"""
    try:
        load_dotenv()
        logger.info("Variables de entorno cargadas desde .env")
    except ImportError:
        logger.warning("python-dotenv no está instalado, no se cargarán variables desde .env")

def main(mode=None):
    """
    Función principal que decide si hacer carga inicial o actualización diaria.

    Args:
        mode (str): Modo de ejecución: 'initial', 'daily' o None (automático)
    """
    # Cargar variables de entorno
    load_env_variables()

    # Verificar token de API
    try:
        get_esios_token()
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    # Usar la cadena de conexión construida
    db_connection = db_connection_string
    if not db_connection:
        # Usar SQLite por defecto si no se puede construir la cadena de conexión
        logger.warning("No se pudo construir la cadena de conexión. Usando SQLite local.")
        db_connection = "sqlite:///data/energyprices.db"
    
    # Decidir modo de ejecución
    if mode == 'initial':
        perform_initial_load(db_connection)
    elif mode == 'daily':
        perform_daily_update(db_connection)
    else:
        # Modo automático: verificar si necesitamos carga inicial
        if check_initial_load_needed(db_connection):
            perform_initial_load(db_connection)
        else:
            perform_daily_update(db_connection)

if __name__ == "__main__":
    # Determinar modo de ejecución desde argumentos de línea de comandos
    mode = sys.argv[1] if len(sys.argv) > 1 else None
    main(mode)