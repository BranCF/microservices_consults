#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import pandas as pd
import os
import telegram
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import asyncio


# In[ ]:


load_dotenv();


# In[ ]:


BOT_TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID_1 = os.getenv('CHAT_ID_1')
CHAT_ID_2 = os.getenv('CHAT_ID_2')
CHAT_ID_3 = os.getenv('CHAT_ID_3')
API_KEY = os.getenv('API_KEY_MONGO')
THRESHOLD_MS = int(os.getenv('THRESHOLD_MS'))
THRESHOLD_DUPLICATES = int(os.getenv('THRESHOLD_DUPLICATES'))
with open(os.getenv('MEAN_RECORDS_JSON_MONGO'), 'r') as meanRecordsByHour:
    meanStdRecords = json.load(meanRecordsByHour)


# In[ ]:


USERNAME = os.getenv('USER_MONGO')
PASSWORD = os.getenv('PASSWORD_MONGO')
HOST = os.getenv('HOST_MONGO')
PORT = int(os.getenv('PORT_MONGO'))
DATABASE = os.getenv('DATABASE_MONGO')
COLLECTION = os.getenv('COLLECTION_MONGO')
CA_CERT = os.getenv('CA_CERT_MONGO')
CLIENT_CERT = os.getenv('CLIENT_CERT_MONGO')


# In[ ]:


bot = telegram.Bot(token=BOT_TOKEN)

async def send_telegram_message(message, chat_semaphore):
    '''
    Envía un mensaje de alerta a Telegram (asincrónico).
    '''
    try:
        await bot.send_message(chat_id=chat_semaphore, text=message)
        print(f"Mensaje enviado a Telegram: {message}")
    except Exception as e:
        print(f"Error al enviar mensaje a Telegram: {e}")
def connect_to_mongo():
    '''
    Establece la conexión a MongoDB y devuelve el cliente y la colección.
    '''
    try:
        client = MongoClient(
            host=f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/?authSource=admin",
            tls=True,
            tlsCAFile=CA_CERT,
            tlsCertificateKeyFile=CLIENT_CERT
        )
        db = client[DATABASE]
        collection = db[COLLECTION]
        print(collection)
        return collection
    except ConnectionFailure as e:
        print("Failed to connect to MongoDB:", e)
        return None

def fetch_data_from_mongo(collection, required_fields, time_field="responseTime", time_range_minutes=10):
    """
    Extrae datos de MongoDB aplicando un filtro de tiempo relativo a los datos existentes y seleccionando solo las columnas necesarias.
    """

    try:
        #max_time_doc = collection.find_one({}, {time_field: 1}, sort=[(time_field, -1)])
        #if not max_time_doc or time_field not in max_time_doc:
        #    print(f"No se encontró el campo '{time_field}' en la base de datos.")
         #   return []    
        #max_time = max_time_doc[time_field]

        max_time =  datetime.now()
        start_time = max_time - timedelta(minutes=time_range_minutes)
        end_time = max_time
        query = {time_field: {"$gte": start_time, "$lte": end_time}}
        projection = {field: 1 for field in required_fields} if required_fields else None
        documents = list(collection.find(query, projection))
        return documents

    except Exception as e:
        print(f"Error al extraer datos desde MongoDB: {e}")
        return []

def flatten_json(json_object, parent_key='', sep='_'):
    """
    Aplana un JSON anidado para convertirlo en una estructura plana.
    """
    items = []
    if isinstance(json_object, dict):
        for k, v in json_object.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_json(v, new_key, sep=sep).items())
            elif isinstance(v, list) and all(isinstance(i, (str, int, float)) for i in v):
                items.append((new_key, ', '.join(map(str, v))))
            elif isinstance(v, list):
                for i, sub_item in enumerate(v):
                    items.extend(flatten_json(sub_item, f"{new_key}{sep}{i}", sep=sep).items())
            else:
                items.append((new_key, v))
    elif isinstance(json_object, list):
        for i, sub_item in enumerate(json_object):
            items.extend(flatten_json(sub_item, f"{parent_key}{sep}{i}", sep=sep).items())
    return dict(items)

async def analyze_service_status(df):
    """
    Analiza el estado de servicio para detectar errores HTTP y envía alertas por cada tipo de error.
    """
    total_rows = len(df)
    if total_rows == 0:
        message = "¡ALERTA_ROJA__c_ciudadana! No hay datos para analizar."
        await send_telegram_message(message, CHAT_ID_3)
        return {"failure_rate": 0, "total_rows_analyzed": 0}
    error_codes = [500, 401, 403, 404]  
    error_counts = {}
    for code in error_codes:
        error_rows = df[df['responseCode'] == code]
        error_counts[code] = len(error_rows)

        if len(error_rows) == 1:
            message = f"¡ALERTA_AMARILLA__c_ciudadana! {len(error_rows)} consultas fallidas con error {code}."
            print(message)
            await send_telegram_message(message, CHAT_ID_2)
        elif len(error_rows) > 1:
            message = f"¡ALERTA_ROJA_c_ciudadana! {len(error_rows)} consultas fallidas con error {code}."
            print(message)
            await send_telegram_message(message, CHAT_ID_3)

    total_failures = sum(error_counts.values())
    failure_rate = (total_failures / total_rows * 100) if total_rows > 0 else 0
 
    return {
        "failure_rate": failure_rate,
        "total_rows_analyzed": total_rows,
        "error_counts": error_counts,
    }
 
async def check_high_latency(df, column_name, threshold_ms):
    threshold = float(threshold_ms)  
    high_latency = df[df[column_name] > threshold]
 
    if len(high_latency) == 1:
        message = f"¡ALERTA_AMARILLA_c_ciudadana! {len(high_latency)} consultas con latencia mayor a {threshold_ms} ms."
        print(message)
        await send_telegram_message(message, CHAT_ID_2)
    elif len(high_latency)>1:
        message = f"¡ALERTA_ROJA_c_ciudadana! {len(high_latency)} consultas con latencia mayor a {threshold_ms} ms."
        print(message)
        await send_telegram_message(message, CHAT_ID_3)
        
    return len(high_latency)
 
async def check_duplicate_locations(df, lat_column, lon_column, threshold, exclude_value=1):
    df[lat_column] = pd.to_numeric(df[lat_column], errors='coerce')
    df[lon_column] = pd.to_numeric(df[lon_column], errors='coerce')
 
    filtered_df = df[(df[lat_column] != exclude_value) & (df[lon_column] != exclude_value)]
    grouped = (
        filtered_df.groupby([lat_column, lon_column])
        .size()
        .reset_index(name='count')
    )
    duplicates = grouped[grouped['count'] > threshold]
 
    if not duplicates.empty:
        message = f"¡ALERTA_AMARILLA_c_ciudadana! {len(duplicates)} ubicaciones con más de {threshold} solicitudes."
        print(message)
        await send_telegram_message(message, CHAT_ID_2)
 
    return duplicates
 
async def check_invalid_api_keys(df, column_name, valid_api_key):
    invalid_keys = df[df[column_name] != valid_api_key]
 
    if not invalid_keys.empty:
        message = f"¡ALERTA_AMARILLA_c_ciudadana! {len(invalid_keys)} registros contienen apiKey inválida."
        print(message)
        await send_telegram_message(message, CHAT_ID_2)
 
    return invalid_keys

async def counts_records(df, meanStdRecords, last_time_sub, n_minutes, nombreMicroservicio = 'CONSULTA_CIUDADANA'):   
    '''
    Counts how many records dataframe has and determines if the count is normal
    Args:
        regularizationFrame (DataFrame): the dataframe to search
        meanStdRecords (dict): Dictionary with traffic count averages and standard deviations by day and hour
        lastTimeSub (string): Day and hour of comparation
        days_of_week : Days of week list
        n_minutes: Difference between the older record and the newer record 
    Returns:
    '''
    
    days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    date_object = datetime.strptime(str(last_time_sub), "%Y-%m-%d %H:%M:%S.%f")
    
    day_name = date_object.strftime("%A")  
    hour = date_object.hour 
    
    meanStd = meanStdRecords[f'{day_name}_{hour}']
    
    std = meanStd['std']
    mean = meanStd['mean']

    nMinutesProportion = n_minutes/60
    
    if std/mean >0.6:
        maxRecords = nMinutesProportion*(mean + 2*std)
        minRecords = nMinutesProportion*(mean - 2*std)
    else:
        maxRecords = nMinutesProportion*(mean + 1.5*std)
        minRecords = nMinutesProportion*(mean - 1.5*std)
        
    if minRecords < 0:
        minRecords = 0
    
    recordsNumber = df.shape[0]
    
    if recordsNumber> maxRecords and recordsNumber/maxrecords < 2:
        message = f'Alerta media {nombreMicroservicio} de cantidad de registros. Se registran más de {maxRecords} para la hora: {recordsNumber}'
        await send_telegram_message(message, CHAT_ID_2)
    elif recordsNumber/maxRecords >= 2:
        message = f'Alerta alta {nombreMicroservicio} de cantidad de registros. Se registran más de {maxRecords} para la hora: {recordsNumber}'
        await send_telegram_message(message, CHAT_ID_3)
    else:
        message = f'Sin alerta {nombreMicroservicio}. El tráfico no rebasa la cota superior.'
        await send_telegram_message(message, CHAT_ID_1)
        
    if recordsNumber< minRecords and minRecords/recordsNumber < 2:
        message = f'Alerta media {nombreMicroservicio} de cantidad de registros. Se registran menos de {minRecords} para la hora: {recordsNumber}'
        await send_telegram_message(message, CHAT_ID_2)
    elif minRecords/recordsNumber >= 2:
        message = f'Alerta alta {nombreMicroservicio} de cantidad de registros. Se registran menos de {minRecords} para la hora: {recordsNumber}'
        await send_telegram_message(message, CHAT_ID_3)
    else:
        message = f'Sin alerta {nombreMicroservicio}. El tráfico no rebasa la cota inferior.'
        await send_telegram_message(message, CHAT_ID_1) 
 
async def main():
    collection = connect_to_mongo()
    if collection is None:
        await send_telegram_message("¡Alerta_ROJA_c_ciudadana! No se pudo conectar a MongoDB.", CHAT_ID_3)
        return
 
    #required_fields = ["responseTime", "requestTime", "responseCode", "latitude","longitude", "apiKey"]
    required_fields = ["responseTime", "requestTime", "responseCode", "apiKey"]
    
    raw_data = fetch_data_from_mongo(collection, required_fields=required_fields, time_field="responseTime", time_range_minutes=10)
   
    if not raw_data:
        await send_telegram_message("¡Alerta_ROJA_c_ciudadana! No se encontraron datos en los últimos 10 minutos (relativos).", CHAT_ID_3)
        return
 
    flattened_data = [flatten_json(doc) for doc in raw_data]
    df = pd.DataFrame(flattened_data)
    last_time_sub = df['responseTime'].iloc[-1] 
 
    columns_to_convert = ['requestTime', 'responseTime']
    for col in columns_to_convert:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
        else:
            print(f"Advertencia: La columna '{col}' no está presente en los datos.")
 
    if 'requestTime' in df.columns and 'responseTime' in df.columns:
        df['tie_comu_ini'] = (df['responseTime'] - df['requestTime']).dt.total_seconds() * 1000
    else:
        await send_telegram_message("¡Alerta_AMARILLA_c_ciudadana! Error: No se pueden calcular tiempos porque faltan 'requestTime' o 'responseTime'.", CHAT_ID_2)
        return
 
    total_consultas = len(df)
    await send_telegram_message(f"¡Alerta_VERDE_c_ciudadana! Total de consultas evaluadas: {total_consultas}", CHAT_ID_1)
 
    await analyze_service_status(df)
    await check_high_latency(df, column_name='tie_comu_ini', threshold_ms=THRESHOLD_MS)
    if 'latitude' in list(df.columns) and 'longitude' in list(df.columns):
        await check_duplicate_locations(
            df, lat_column='latitude', lon_column='longitude', threshold=THRESHOLD_DUPLICATES
        )
    await check_invalid_api_keys(df, column_name='apiKey', valid_api_key=API_KEY)
    await counts_records(df, meanStdRecords, last_time_sub, 10)


# In[ ]:


asyncio.run(main())

