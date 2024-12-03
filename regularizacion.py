#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import numpy as np
import pandas as pd
import json
import os
from dotenv import load_dotenv
import pymysql.cursors
import datetime
import telegram
import asyncio


# In[ ]:


load_dotenv();


# In[ ]:


ssl_config = {
    "ssl_ca": os.getenv('CA-CERTIFICATE'),
}
    
db_config = {
    "user": os.getenv('USER_MYSQL'),
    "password": os.getenv('PASSWORD'),
    "host": os.getenv('HOST'),  
    "port": int(os.getenv('PORT')),
    "database": os.getenv('DATABASE'),
    "ssl":ssl_config
} 

def connectToDatabase(db_config):
    '''
    Connects to a MySQL database
    Args:
        db_config (dict): Dictionary with user, password, host, port, database name and ssl certs and keys to connect
    Returns:
      conn (pymsql connection object)
    '''
    try:    
        conn = pymysql.connect(**db_config)    
        return conn
    except pymysql.Error as err:
        return err

def connectHealth(db_config):
    '''
    Connects to a database and makes a proof query just to know everything's good
    Args:
        db_config (dict): Dictionary with user, password, host, port, database name and ssl certs and keys to connect
    Returns:
      ...(string): A message printing the query results
    '''
    try:
        conn = connectToDatabase(db_config)
        cursor = conn.cursor() 
        cursor.execute('SHOW TABLES;')
        result = cursor.fetchall()
        disconnectToDatabase(conn, cursor)
        return f"Connected to database: {list(result)}"  
    except AttributeError as atr_err:
        
        return atr_err
    

def disconnectToDatabase( conn, cursor):
    '''
    Disconnects to a database
    Args:
        conn
        cursor
    Returns:
    '''
    cursor.close()
    conn.close()

def getTheLastDate(db_config: dict, view: str):
    
    '''
    Searches the first row of a MySQL view where its response time column value is the most recent
    Args:
        db_config (dict): Connection dictionary
        view (string): View's name
    Returns:
        result (tuple): The row mentioned
    '''
    try:
         conn = connectToDatabase(db_config)
         cursor = conn.cursor() 
         #query = f"SELECT * FROM {view} ORDER BY STR_TO_DATE(responseTime, '%Y-%m-%d %H:%i:%s') DESC;"
         query = f"SELECT * FROM {view} ORDER BY responseTime DESC;"
         cursor.execute(query)
         result = cursor.fetchone()
         disconnectToDatabase(conn, cursor)
         return result
    except AttributeError as atr_err:
         return atr_err

def getTheDataframe(db_config: dict, view: str, lastTimeSub:int, lastTime: str):
    '''
    Searches the rows of a MySQL view where its response time column values are between lastTime and lastTimeSub
    Args:
        db_config (dict): Connection dictionary
        view (string): View's name
        lastTimeSub (string): The older date
        lastTIme (string): The newer date
    Returns:
        result (tuple): The rows mentioned
    '''
    try:  
        conn = connectToDatabase(db_config)
        cursor = conn.cursor() 
        #query = f"SELECT *FROM {view} WHERE STR_TO_DATE(responseTime, '%Y-%m-%d %H:%i:%s') BETWEEN STR_TO_DATE('{lastTimeSub}', '%Y-%m-%d %H:%i:%s') AND STR_TO_DATE('{lastTime}', '%Y-%m-%d %H:%i:%s') ORDER BY STR_TO_DATE(responseTime, '%Y-%m-%d %H:%i:%s') DESC;"
        query = f"SELECT *FROM {view} WHERE responseTime BETWEEN '{lastTimeSub}' AND '{lastTime}' ORDER BY responseTime DESC;"
        cursor.execute(query)
        result = cursor.fetchall()
        disconnectToDatabase(conn, cursor)
        return result
    except AttributeError as atr_err:
        return atr_err   


# In[ ]:


#Private
def getRepeatedValuesInAColumn(df, columnName):
    '''
    Identifies the repeated values of a column in a dataframe
    Args:
       df (DataFrame) : Dataframe which contains the columns to search
       columnName (string) : Column's name to search
    Returns:
       duplicates['VIN'] (pandas Series): Returns a column as a pandas series of the duplicated values of the columnName
    '''
    duplicates = df[df.duplicated(subset=[columnName], keep=False)]
    return duplicates['VIN']

#Private
def searchValuesInADataframe(df, values:list, columnA: str, columnB: str)-> dict:
    '''
    Optimized version to search values in a dataframe columnA and retrieve the corresponding values of columnB.
    Args:
       values (list): The list of values to be searched.
       columnA (str): The name of the column to search.
       columnB (str): The name of the column to retrieve values from.
       df (DataFrame): The dataframe to search in.
    Returns:
      grouped: A dictionary with keys as the values from columnA and values as the list of corresponding entries from columnB.
    '''
    
    filtered_df = df[df[columnA].isin(values)]

    grouped = filtered_df.groupby(columnA)[columnB].apply(list).to_dict()

    return grouped   


def verifyInfoRepeatedVins(df):
    '''
    Verifies every repeated VIN has the same information in every record (due to the short period time reviewed)
    Args:
       df (DataFrame): The dataframe where where will be searched
    Returns:
      ... (dict): A dictionary which contains the different responses as a dict for every repeated VIN in uniqueResponses and VINs with more than one response in differentResponse key
    '''
    uniqueResponses = {}
    differentResponses = {}
    vinValues = getRepeatedValuesInAColumn(df, 'VIN').unique().tolist()
    repeatedValues = searchValuesInADataframe(df, vinValues, 'VIN', 'responseBody')
    repeatedVins = repeatedValues.keys()
    for vin in repeatedVins:
        uniqueResponses[vin] = []
        listJson = []
        for responseString in repeatedValues[vin]:
            responseJson = json.loads(responseString)
            listJson.append(responseJson)
        repeatedValues[vin] = listJson
    for vin in repeatedVins:
        for responseJson in repeatedValues[vin]:
            keys = list(responseJson.keys())
            info = {}
            if 'anioModelo' in keys:
                if 'fabricante' in keys and 'paisOrigen' in keys:
                    info = {'anioModelo': responseJson['anioModelo'], 'fabricante': responseJson['fabricante'], 'marca': responseJson['marca'], 'modelo': responseJson['modelo'], 'paisOrigen': responseJson['paisOrigen'], 'robo': responseJson['robo'], 'roboFecha': responseJson['roboFecha'], 'codes': []}
                else:
                     info = {'anioModelo': responseJson['anioModelo'], 'marca': responseJson['marca'], 'modelo': responseJson['modelo'], 'robo': responseJson['robo'], 'roboFecha': responseJson['roboFecha'], 'codes': []}
            if 'mensajes' in keys and type(responseJson['mensajes'])== 'list' and responseJson['mensajes'] != []:
                    for message in responseJson['mensajes']:
                        if 'codes' in list(message.keys()):
                            info['codes'].append(message['codigo'])
                            info['codes'] = list(set(info['codes']))
                    if info.get('codes')!= None and len(info['codes'])>1:
                        info['codes'] = info['codes'].sort()
            if uniqueResponses[vin] == []:
                 uniqueResponses[vin].append(info)
            else:
                if info not in uniqueResponses[vin]:
                    uniqueResponses[vin].append(info)
                    differentResponses[vin] = info
    return {'uniqueResponses': uniqueResponses, 'differentResponses': differentResponses}

async def distinctValueVin(regularizationFrame):
    '''
    Compares the repeated VIN's values and determines if they are equal for each one
    Args:
        regularizationFrame (DataFrame): the dataframe to search
    Returns:
    '''
    differentResponses = verifyInfoRepeatedVins(regularizationFrame)['differentResponses']
    responsesKeys = differentResponses.keys()
    
    if len(responsesKeys) == 1:
        message = f'Alerta media {nombreMicroservicio}: Existe VIN con distintos valores para una consulta{responsesKeys[0]}:{differentResponses[responsesKeys[0]]}'
        await send_telegram_message(message, chat_id_yellow)
    elif len(responsesKeys)>1:
        message = f'Alerta alta {nombreMicroservicio}: Existen VINs con distintos valores para una consulta {responsesKeys}'
        await send_telegram_message(message, chat_id_red)
    else:
        message = f'Sin alerta {nombreMicroservicio} de incongruencias entre VINs repetidos'
        await send_telegram_message(message, chat_id_green)

def proveErrorAlert(df):
    '''
    Assures every error in CarfaxUsaData is associated with an alert
    Args:
       df (DataFrame) : Dataframe which contains the columns to substraction
    Returns:
       ... (int): Returns a count
    '''
    carfaxUsaData = df['carfaxUsaData'].values.tolist()
    conError = 0
    conErrorSinAlerta = 0
    for index1 in range(len(carfaxUsaData)):
        carfaxDict = json.loads(carfaxUsaData[index1])
        if 'error' in list(carfaxDict.keys()):
            conError += 1
            if df.iloc[index1]['alertas'] == []:
                conErrorSinAlerta+=1
    return conErrorSinAlerta

async def carfaxAlertsRelation(regularizationFrame, maxCarfaxAlerts):
    '''
    Determines if every VIN with error in carfaxUsaData has an associated alert
    Args:
        regularizationFrame (DataFrame): the dataframe to search
        maxCarfaxAlerts: Max number of VIN's with and without alerts
    Returns:
    '''
    carfaxAlerts = proveErrorAlert(regularizationFrame)
    if carfaxAlerts == maxCarfaxAlerts:
        message = f'Alerta media {nombreMicroservicio}: Existen {carfaxAlerts} registros con error y sin alerta'
        await send_telegram_message(message, chat_id_yellow)
    elif carfaxAlerts > maxCarfaxAlerts:
        message = f'Alerta alta {nombreMicroservicio}: Existen {carfaxAlerts} registros con error y sin alerta'
        await send_telegram_message(message, chat_id_red)
    else:
        message = f'Sin alerta {nombreMicroservicio} entre mensajes carfaxUsaData y Alertas'
        await send_telegram_message(message, chat_id_green)
        
async def httpCodesAlerts(regularizationFrame, maxCodeAlerts, errorCodes):
    '''
    Counts how many http error codes dataframe has
    Args:
        regularizationFrame (DataFrame): the dataframe to search
        maxCodeAlerts: Max number of allowed alerts
        errorCodes: Definition of error codes
    Returns:
    '''
    regularizationFrame['responseCode'] = regularizationFrame['responseCode'].astype(int)
    bad_codes = regularizationFrame[regularizationFrame['responseCode'] >= errorCodes]
    
    bad_code_counts = bad_codes['responseCode'].value_counts()
    
    for code, count in bad_code_counts.items():
        if count == 2:
            message = f'Alerta media {nombreMicroservicio} de errores HTTP con código {code}'
            await send_telegram_message(message, chat_id_yellow)
        elif count > 2:
            message = f'Alerta alta {nombreMicroservicio} de errores HTTP con código {code} del microservicio {nombreMicroservicio}'
            await send_telegram_message(message, chat_id_red)
    
        if list(bad_code_counts)==[]:
            message = f'Sin errores {nombreMicroservicio} de código HTTP'
            await send_telegram_message(message, chat_id_green)        

async def recordsAlerts(regularizationFrame, meanStdRecords, lastTimeSub, n_minutes):   
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
    
    key = f'{days_of_week[lastTimeSub.weekday()]}_{lastTimeSub.hour}'
    
    meanStd = meanStdRecords[f'{days_of_week[lastTimeSub.weekday()]}_{lastTimeSub.hour}']
    
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
    
    recordsNumber = regularizationFrame.shape[0]
    
    if recordsNumber> maxRecords and recordsNumber/maxrecords < 2:
        message = f'Alerta media {nombreMicroservicio} de cantidad de registros. Se registran más de {maxRecords} para la hora: {recordsNumber}'
        await send_telegram_message(message, chat_id_yellow)
    elif recordsNumber/maxRecords >= 2:
        message = f'Alerta alta {nombreMicroservicio} de cantidad de registros. Se registran más de {maxRecords} para la hora: {recordsNumber}'
        await send_telegram_message(message, chat_id_red)
    else:
        message = f'Sin alerta {nombreMicroservicio}. El tráfico no rebasa la cota superior.'
        await send_telegram_message(message, chat_id_green)
        
    if recordsNumber< minRecords and minRecords/recordsNumber < 2:
        message = f'Alerta media {nombreMicroservicio} de cantidad de registros. Se registran menos de {minRecords} para la hora: {recordsNumber}'
        await send_telegram_message(message, chat_id_yellow)
    elif minRecords/recordsNumber >= 2:
        message = f'Alerta alta {nombreMicroservicio} de cantidad de registros. Se registran menos de {minRecords} para la hora: {recordsNumber}'
        await send_telegram_message(message, chat_id_red)
    else:
        message = f'Sin alerta {nombreMicroservicio}. El tráfico no rebasa la cota inferior.'
        await send_telegram_message(message, chat_id_green) 

def calculateDeltaTime(df, columnA, columnB):
    '''
    Converts columnA and columnB in datetime type and makes the difference between both columns
    Args:
       df (DataFrame) : Dataframe which contains the columns to substraction
       columnA (string) : Minuend column
       columnB (string): Substrahend column
    Returns:
       responsePeriod (DataFrame): Returns a column of the seconds difference as a dataframe
    '''
    responseTimeType = pd.to_datetime(df[columnA], format='%Y-%m-%d %H:%M:%S')
    requestTimeType = pd.to_datetime(df[columnB], format='%Y-%m-%d %H:%M:%S')
    responsePeriod = (responseTimeType - requestTimeType).to_frame()
    responsePeriod['secondsDifference'] = pd.to_timedelta(responsePeriod[0]).dt.total_seconds()
    responsePeriod.drop([0], axis='columns', inplace=True)

    return responsePeriod

async def delayAlerts(regularizationFrame, maxCountDelay):
    '''
    Counts how many slow response records dataframe has and determines if the count is normal
    Args:
        regularizationFrame (DataFrame): the dataframe to search
        maxCountDelay (int): Max number of allowed slow records
    Returns:
    '''
    timeFrame = calculateDeltaTime(regularizationFrame[(regularizationFrame['responseTime']!='xD')&(regularizationFrame['requestTime']!='xD')], 'responseTime', 'requestTime')
    
    timeMax = timeFrame['secondsDifference'].max()
    
    if timeMax > timeMaximum:
        
        regularizationFrame['secondsDifference'] = timeFrame['secondsDifference']
        slowtime = regularizationFrame[regularizationFrame['secondsDifference'] > timeMaximum]
        slowtime_counts = slowtime['secondsDifference'].value_counts()
        count = int(slowtime_counts.sum())
    
        if count == maxCountDelay:
            message = f'Alerta media {nombreMicroservicio} de lentitud: {count} registros lentos'
            await send_telegram_message(message, chat_id_yellow)
        elif count >ma2:
            message = f'Alerta alta {nombreMicroservicio} de lentitud: {count} registros lentos'
            await send_telegram_message(message, chat_id_red)
    else:
        message = f'Sin alerta {nombreMicroservicio} de lentitud'
        await send_telegram_message(message, chat_id_green)    

async def apiKeysAlerts(regularizationFrame, apiKeyMax, apiKey):
    '''
    Counts how many different apiKey records dataframe has and determines if the count is normal
    Args:
        regularizationFrame (DataFrame): the dataframe to search
        apiKeyMax (int): Max number of allowed different apiKey records
        apiKey(string): allowed apiKey
    Returns:
    '''
    
    bad_apiKeys = regularizationFrame[regularizationFrame['apiKey'] != apiKey]
    
    bad_apiKeys_counts = bad_apiKeys['apiKey'].value_counts()
    
    for apiKey, count in bad_apiKeys_counts.items():
        if count == apiKeyMax:
            message = f'Alerta media {nombreMicroservicio} para la api Key {apiKey}'
            await send_telegram_message(message, chat_id_yellow)
        elif count > apiKeyMax:
            message = f'Alerta alta {nombreMicroservicio} para la apiKey {apiKey}'
            await send_telegram_message(message, chat_id_red)
    
    if list(bad_apiKeys_counts)==[]:
        message = f'Sin alerta {nombreMicroservicio} con la apiKey'
        await send_telegram_message(message, chat_id_green)
    
async def ipAlerts(regularizationFrame, ipMaximum):
    '''
    Counts how many vins a ip consulted and determines if the count is normal
    Args:
        regularizationFrame (DataFrame): the dataframe to search
        ipMaximum (int): Max number of allowed VIN's per ip
    Returns:
    '''
    ips_vins_frame = (
        regularizationFrame.groupby('ip')['VIN']
        .nunique()  
        .reset_index()  
        .rename(columns={'VIN': 'count'})  
    )
    ipsMax = int(ips_vins_frame['count'].max())
    suspiciousIps = list(ips_vins_frame[ips_vins_frame['count'] == ipsMax]['ip'])
    if ipsMax > ipMaximum:
        message = f'Alerta media {nombreMicroservicio} de ips que checan más de 20 VINs distintos {suspiciousIps} con cantidad: {ipsMax}'
        await send_telegram_message(message, chat_id_yellow)
    else:
        message = f'Sin alertas {nombreMicroservicio} para IPs con máximos: {suspiciousIps} con cantidad: {ipsMax}'
        await send_telegram_message(message, chat_id_green)  

async def connectionHealth(connection):
    '''
    Send a message by telegram with the connection status 
    Args:
        connection
    Returns:
    '''
    print(connection)
    if type(connection)==AttributeError:
        message = f'{nombreMicroservicio}: No ha sido posible conectarse a la base de datos: {connection}'
        await send_telegram_message(message, chat_id_red)
    else:
        message = f'{nombreMicroservicio}: Conectado a la base de datos'
        await send_telegram_message(message, chat_id_green)    
        

async def send_telegram_message(message, chat_id_semaphore):
    """
    Send an alert message (async).
    
    Args:
        message (str): Message to send
        chat_id_semaphore (str): Telegram id of chat to send the message
    """
    try:
        await bot.send_message(chat_id=chat_id_semaphore, text=message)
        print(f"Mensaje enviado a Telegram: {message}")
    except Exception as e:
        print(f"Error al enviar mensaje a Telegram: {e}")


# In[ ]:


view = os.getenv('VIEW_NAME')
n_minutes = int(os.getenv('DIFFERENCE_MINUTES'))
apiKey = os.getenv('API_KEY_REGULARIZATION')
ipMaximum = 20
maxCarfaxAlerts = 2
maxCodeAlerts = 2
apiKeyMax = 2
maxCountDelay = 2
nombreMicroservicio = 'REGULARIZACIÓN'
errorCodes = 500
timeMaximum = 6
labels = ['VIN', '_id', 'host', 'ip', 'apiKey', 'userId', 'idReporte',
           'requestParameters', 'requestUrl', 'responseCode', 'responseCodeStatus',
           'requestTime', 'responseTime', 'labels', 'idRespuesta', 'idConsulta',
           'responseBody', 'carfaxUsaData', 'alertas', 'firewallUsa']
with open(os.getenv('MEAN_RECORDS_JSON'), 'r') as meanRecordsByHour:
    meanStdRecords = json.load(meanRecordsByHour)


# In[ ]:


bot = telegram.Bot(token=os.getenv('BOT_TOKEN'))
chat_id_green = os.getenv('CHAT_ID_GREEN')
chat_id_yellow = os.getenv('CHAT_ID_YELLOW')
chat_id_red = os.getenv('CHAT_ID_RED')


# In[ ]:


async def regularizationAnalysis():
    await connectionHealth(connectHealth(db_config))
    
    lastTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    lastTimeSub = (datetime.datetime.now() - datetime.timedelta(minutes = n_minutes)).strftime('%Y-%m-%d %H:%M:%S')
    #lastTime = list(getTheLastDate(db_config, view))[12]
    #lastTimeSub = (list(getTheLastDate(db_config, view))[12] - datetime.timedelta(minutes = n_minutes))
    
    resultQueryList = list(getTheDataframe(db_config, view, lastTimeSub.strftime('%Y-%m-%d %H:%M:%S'), lastTime.strftime('%Y-%m-%d %H:%M:%S')))
    regularizationFrame = pd.DataFrame(resultQueryList, columns = labels)
    regularizationFrame = regularizationFrame.fillna('xD')
    
    await ipAlerts(regularizationFrame, ipMaximum)
    
    await httpCodesAlerts(regularizationFrame, maxCodeAlerts, errorCodes)
    
    await apiKeysAlerts(regularizationFrame, apiKeyMax, apiKey)
    
    await delayAlerts(regularizationFrame, maxCountDelay)
    
    await recordsAlerts(regularizationFrame, meanStdRecords, lastTimeSub, n_minutes)
    
    await carfaxAlertsRelation(regularizationFrame, maxCarfaxAlerts)
    
    await distinctValueVin(regularizationFrame)


# In[ ]:


asyncio.run(regularizationAnalysis())

#while True:
#    await regularizationAnalysis()
#    time.sleep(int(os.getenv('DIFFERENCE_MINUTES'))*60)


# In[ ]:




