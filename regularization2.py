#!/usr/bin/env python
# coding: utf-8

# In[25]:


import numpy as np
import pandas as pd
import json
import os
from dotenv import load_dotenv
import pymysql.cursors
import datetime
import telegram
import schedule
import time
import asyncio
import aioschedule


# In[2]:


load_dotenv();


# In[3]:


class DataFrame_analysis:
    '''
    This class contains every method for regularization microservice function analysis
    '''
    #Class variables

    #The init method 
    def __init__(self):
        pass

    #Public
    def getDfSize(self, df):
        '''
        Returns the rows and columns of a dataframe
        Args:
           df (Dataframe) : Dataframe to analyze
        Returns:
          ... (dictionary): A dictionary with rows and columns as keys
        '''
        rows = len(df.axes[0])
        columns = len(df.axes[1])
        return {'rows': rows, 'columns': columns}

    #Public
    def determineDistinctValues(self, df, col):
        '''
        Determines the unrepeated values in a columns
        Args:
           df (DataFrame) : Dataframe which contains the column to search
           col (string) : Column's name to search
        Returns:
           df[col].unique() (array): An array (iterable object) with the unrepeated values of the column
        '''
        return df[col].unique()

    #Public
    def determineDistinctKeys(self, columnToList): #Recibe una lista, no una dataframe column
        '''
        Extracts the keys of a dictionary list and saves it in a dictionary with its count of appearences in the list
        Args:
           columnList (list) : List of dictionary to extract the keys
        Returns:
           distinctKeys (dictionary): A dictionary with the keys as a string and its appearences count in the list
        '''
        distinctKeysDict = {}
        distinctKeysList = []
        for dictionary in columnToList:
            if str(dictionary)[0] == "{" :
                dictionary = json.loads(dictionary)
                orderedKeys = sorted(list(dictionary.keys()))
                if orderedKeys in distinctKeysList:
                    distinctKeysDict['/'.join(orderedKeys)] += 1
                else:
                    distinctKeysList.append(orderedKeys)
                    distinctKeysDict['/'.join(orderedKeys)] = 1
            else:
                if ''.join(['notKey:',dictionary]) in distinctKeysList:
                    distinctKeysDict[''.join(['notKey:',dictionary])] += 1
                else:
                    distinctKeysList.append(''.join(['notKey:',dictionary]))
                    distinctKeysDict[''.join(['notKey:',dictionary])] = 1
        return distinctKeysDict     

    #Public
    def countAlertCodes(self, df, alertsColumn: str):
        '''
        Counts the alert codes in a column of a dataframe
        Args:
           df (DataFrame) : Dataframe which contains the column to search
           alertsColumn (string) : Column's name to search
        Returns:
           codeCount (dictionary): A dictionary with the distinct alert codes as keys and the appearence count in the column
        '''
        alertsWithCode = filter(lambda x: x != [],df['alertas'].values.tolist())
        codeCount = {'Code 1': 0 , 'Code 2': 0, 'Code 3': 0, 'Code 4': 0, 'Code 5': 0, 'Code 6': 0, 'Code 7': 0, 'invalid': 0}
    
        for alert in alertsWithCode:
            if str(alert)[0] =='[':
                alertList = json.loads(alert)
                if alertList != []:
                    for subalert in alertList:
                        n = subalert['codigo']
                        codeCount[''.join(['Code ', str(n)])] += 1
            else:
               codeCount['invalid']+=1
    
        return codeCount 
    
    #Public
    def calculateDeltaTime(self, df, columnA, columnB):
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

    #Public
    def proveErrorAlert(self, df):
        '''
        Assures every error in CarfaxUsaData is associated with an alert
        Args:
           df (DataFrame) : Dataframe which contains the columns to substraction
        Returns:
           ... (string): Returns a status message
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

    #Private
    def getRepeatedValuesInAColumn(self, df, columnName):
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
    def searchValuesInADataframe(self, df, values:list, columnA: str, columnB: str)-> dict:
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

    #Public
    def getIncorrectRepeatedVinInformation(self, df)->dict:
        '''
        Searches the values of a list in a dataframe columnA and retrieves the information of columnB
        Args:
           df (DataFrame): The dataframe where where will be searched
        Returns:
          ... (dict): A dictionary which contains a status message, a list of good and bad VINs withits appeareance count, and the total count of good and bad VINS
        '''
        repeatedRows = len(self.getRepeatedValuesInAColumn(df, 'VIN').values.tolist())
        vinValues = self.getRepeatedValuesInAColumn(df, 'VIN').unique().tolist()
        repeatedVins = len(vinValues)
        repeatedValues = self.searchValuesInADataframe(df, vinValues, 'VIN', 'alertas')
        goodVins = 0
        badVins = 0
        goodVinsList = []
        badVinsList = []
        for repeatedVin in list(repeatedValues.keys()):
            differentValuesByVin = list(set(repeatedValues[repeatedVin]))
            for differentValue in differentValuesByVin:
                if differentValue == '[]':
                    goodVins += 1
                    if len(differentValuesByVin)>1:
                        return {'message': ''.join(['Alerta con vin: ',repeatedVin, ' , tiene y no tiene alertas.']),
                               'goodVinsList': [],
                               'badVinsList': [],
                              'goodVins':0,
                               'badVins': 0}
                    else:
                        goodVinsList.append([repeatedVin, len(repeatedValues[repeatedVin])])
                else:
                    badVins += 1
                    badVinsList.append([repeatedVin, len(repeatedValues[repeatedVin])])
            

        return {'message': '',
               'goodVinsList': goodVinsList,
               'badVinsList': badVinsList,
               'goodVins':goodVins,
               'badVins': badVins}

    #Public
    def diagnoseVins(self, df):
        '''
        Converts the lists of  getIncorrectRepeatedVinInformation() function in dataframes
        Args:
           df (DataFrame): The dataframe where where will be searched
        Returns:
          ... (dict): A dictionary which contains the good and bad VINs dataframes and the good and bad repeated Records of the VINs
        '''
        goodVinsFrame = pd.DataFrame(self.getIncorrectRepeatedVinInformation(df)['goodVinsList'], columns = ['VIN', 'repeatedTimes'])
        goodRepeatedRecords = goodVinsFrame.sum()['repeatedTimes']
        badVinsFrame = pd.DataFrame(self.getIncorrectRepeatedVinInformation(df)['badVinsList'], columns = ['VIN', 'repeatedTimes'])
        badRepeatedRecords = badVinsFrame.sum()['repeatedTimes']

        return {'goodVinsFrame': goodVinsFrame, 'badVinsFrame': badVinsFrame, 'goodRepeatedRecords': goodRepeatedRecords, 'badRepeatedRecords': badRepeatedRecords}

    #Public
    def verifyInfoRepeatedVins(self, df):
        '''
        Verifies every repeated VIN has the same information in every record (due to the short period time reviewed)
        Args:
           df (DataFrame): The dataframe where where will be searched
        Returns:
          ... (dict): A dictionary which contains the different responses as a dict for every repeated VIN in uniqueResponses and VINs with more than one response in differentResponse key
        '''
        uniqueResponses = {}
        differentResponses = {}
        vinValues = self.getRepeatedValuesInAColumn(df, 'VIN').unique().tolist()
        repeatedValues = self.searchValuesInADataframe(df, vinValues, 'VIN', 'responseBody')
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


# In[4]:


async def send_telegram_message(message, chat_id_semaphore):
    """
    Envía un mensaje de alerta a Telegram (asincrónico).
    
    Args:
        message (str): El mensaje a enviar.
    """
    try:
        bot = telegram.Bot(token=os.getenv('BOT_TOKEN'))
        await bot.send_message(chat_id=chat_id_semaphore, text=message)
        print(f"Mensaje enviado a Telegram: {message}")
    except Exception as e:
        print(f"Error al enviar mensaje a Telegram: {e}")


# In[5]:


def connectToDatabase():
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
    try:    
        conn = pymysql.connect(**db_config)    
        return conn
    except pymysql.Error as err:
        return err

def connectHealth():
    try:
        conn = connectToDatabase()
        cursor = conn.cursor() 
        cursor.execute('SHOW TABLES;')
        result = cursor.fetchall()
        disconnectToDatabase(conn, cursor)
        return f"Connected to database: {list(result)}"
    except AttributeError as atr_err:
        return atr_err
    

def disconnectToDatabase(conn, cursor):
    # Close the cursor and connection
    cursor.close()
    conn.close()

def getTheLastDate(view: str):
     try:
         conn = connectToDatabase()
         cursor = conn.cursor() 
         #query = f"SELECT * FROM {view} ORDER BY STR_TO_DATE(responseTime, '%Y-%m-%d %H:%i:%s') DESC;"
         query = f"SELECT * FROM {view} ORDER BY responseTime DESC;"
         cursor.execute(query)
         result = cursor.fetchone()
         disconnectToDatabase(conn, cursor)
         return result
     except AttributeError as atr_err:
         return atr_err

def getTheDataframe(view: str, lastTimeSub:int, lastTime: str):
     try:
         conn = connectToDatabase()
         cursor = conn.cursor() 
         #query = f"SELECT *FROM {view} WHERE STR_TO_DATE(responseTime, '%Y-%m-%d %H:%i:%s') BETWEEN STR_TO_DATE('{lastTimeSub}', '%Y-%m-%d %H:%i:%s') AND STR_TO_DATE('{lastTime}', '%Y-%m-%d %H:%i:%s') ORDER BY STR_TO_DATE(responseTime, '%Y-%m-%d %H:%i:%s') DESC;"
         query = f"SELECT *FROM {view} WHERE responseTime BETWEEN '{lastTimeSub}' AND '{lastTime}' ORDER BY responseTime DESC;"
         cursor.execute(query)
         result = cursor.fetchall()
         disconnectToDatabase(conn, cursor)
         return result
     except AttributeError as atr_err:
         return atr_err


# In[10]:


async def regularizationAnalysis():
    chat_id_green = os.getenv('CHAT_ID_GREEN')
    chat_id_yellow = os.getenv('CHAT_ID_YELLOW')
    chat_id_red = os.getenv('CHAT_ID_RED')
    
    view = os.getenv('VIEW_NAME')
    n_minutes = int(os.getenv('DIFFERENCE_MINUTES'))
    ipMaximum = int(os.getenv('IPS_MAXIMUM'))
    nombreMicroservicio = 'Regularización'
    errorCodes = 500
    timeMaximum = 6
    days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    #lastTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #lastTimeSub = (datetime.datetime.now() - datetime.timedelta(minutes = n_minutes)).strftime('%Y-%m-%d %H:%M:%S')
    lastTime = list(getTheLastDate(view))[12]
    lastTimeSub = (list(getTheLastDate(view))[12] - datetime.timedelta(minutes = n_minutes))
    
    resultQueryList = list(getTheDataframe(view, lastTimeSub.strftime('%Y-%m-%d %H:%M:%S'), lastTime.strftime('%Y-%m-%d %H:%M:%S')))
    regularizationFrame = pd.DataFrame(resultQueryList, columns = ['VIN', '_id', 'host', 'ip', 'apiKey', 'userId', 'idReporte',
           'requestParameters', 'requestUrl', 'responseCode', 'responseCodeStatus',
           'requestTime', 'responseTime', 'labels', 'idRespuesta', 'idConsulta',
           'responseBody', 'carfaxUsaData', 'alertas', 'firewallUsa'])
    regularizationFrame = regularizationFrame.fillna('xD')
    filteredAnalysis = DataFrame_analysis()
    
    #Alerta de medio nivel de IP's
    ips_vins_frame = (
        regularizationFrame.groupby('ip')['VIN']
        .nunique()  
        .reset_index()  
        .rename(columns={'VIN': 'count'})  
    )
    ipsMax = int(ips_vins_frame['count'].max())
    suspiciousIps = list(ips_vins_frame[ips_vins_frame['count'] == ipsMax]['ip'])
    if ipsMax > ipMaximum:
        message = f'Alerta media de ips que checan más de 20 VINs distintos {suspiciousIps} con cantidad: {ipsMax} del microservicio {nombreMicroservicio}'
        await send_telegram_message(message, chat_id_yellow)
    else:
        message = f'Sin alertas para IPs con máximos: {suspiciousIps} con cantidad: {ipsMax} del microservicio {nombreMicroservicio}'
        await send_telegram_message(message, chat_id_green)
    
    #Alerta de errores de código http. Prioridad media-alta
    regularizationFrame['responseCode'] = regularizationFrame['responseCode'].astype(int)
    
    bad_codes = regularizationFrame[regularizationFrame['responseCode'] >= errorCodes]
    
    bad_code_counts = bad_codes['responseCode'].value_counts()
    
    for code, count in bad_code_counts.items():
        if count == 2:
            message = f'Alerta media de errores HTTP con código {code} del microservicio {nombreMicroservicio}'
            await send_telegram_message(message, chat_id_yellow)
        elif count > 2:
            message = f'Alerta alta de errores HTTP con código {code} del microservicio {nombreMicroservicio}'
            await send_telegram_message(message, chat_id_red)
            
    if list(bad_code_counts)==[]:
        message = f'Sin errores de código HTTP del microservicio {nombreMicroservicio}'
        await send_telegram_message(message, chat_id_green)
    
    #Alerta de errores de uso de api-key. Prioridad media-alta
    apiKey = os.getenv('API_KEY_REGULARIZATION')
    
    bad_apiKeys = regularizationFrame[regularizationFrame['apiKey'] != apiKey]
    
    bad_apiKeys_counts = bad_apiKeys['apiKey'].value_counts()
    
    for apiKey, count in bad_apiKeys_counts.items():
        if count == 2:
            message = f'Alerta media para la api Key {apiKey} del microservicio {nombreMicroservicio}'
            await send_telegram_message(message, chat_id_yellow)
        elif count > 2:
            message = f'Alerta alta para la apiKey {apiKey} del microservicio {nombreMicroservicio}'
            await send_telegram_message(message, chat_id_red)
    
    if list(bad_apiKeys_counts)==[]:
        message = f'Sin errores con la apiKey del microservicio {nombreMicroservicio}'
        await send_telegram_message(message, chat_id_green)
    
    #ALerta de tardanza en respuesta
    timeFrame = filteredAnalysis.calculateDeltaTime(regularizationFrame[(regularizationFrame['responseTime']!='xD')&(regularizationFrame['requestTime']!='xD')], 'responseTime', 'requestTime')
    
    timeMax = timeFrame['secondsDifference'].max()
    
    if timeMax > timeMaximum:
        
        regularizationFrame['secondsDifference'] = timeFrame['secondsDifference']
        slowtime = regularizationFrame[regularizationFrame['secondsDifference'] > timeMaximum]
        slowtime_counts = slowtime['secondsDifference'].value_counts()
        count = int(slowtime_counts.sum())
    
        if count == 2:
            message = f'Alerta media de lentitud del microservicio {nombreMicroservicio}'
            await send_telegram_message(message, chat_id_yellow)
        elif count > 2:
            message = f'Alerta alta de lentitud del microservicio {nombreMicroservicio}'
            await send_telegram_message(message, chat_id_red)
    else:
        message = f'Sin reporte de lentitud del microservicio {nombreMicroservicio}'
        await send_telegram_message(message, chat_id_green)
    
    #Alerta de cantidad de peticiones a una hora determinada
    with open(os.getenv('MEAN_RECORDS_JSON'), 'r') as meanRecordsByHour:
        meanStdRecords = json.load(meanRecordsByHour)
    
    key = f'{days_of_week[lastTimeSub.weekday()]}_{lastTimeSub.hour}'
    
    
    meanStd = meanStdRecords[f'{days_of_week[lastTimeSub.weekday()]}_{lastTimeSub.hour}']
    
    std = meanStd['std']
    mean = meanStd['mean']
    
    if std/mean >0.6:
        maxRecords = mean/6 + (1/3)*std
        minRecords = mean/6 - (1/3)*std
    else:
        maxRecords = mean/6 + (1/4)*std
        minRecords = mean/6 - (1/4)*std
        
    if minRecords < 0:
        minRecords = 0
    
    recordsNumber = regularizationFrame.shape[0]
    
    if recordsNumber> maxRecords and recordsNumber/maxrecords < 2:
        message = f'Alerta media de cantidad de registros. Se registran más de {maxRecords} para la hora: {recordsNumber} del microservicio {nombreMicroservicio}'
        await send_telegram_message(message, chat_id_yellow)
    elif recordsNumber> maxRecords and recordsNumber/maxrecords >= 2:
        message = f'Alerta alta de cantidad de registros. Se registran más de {maxRecords} para la hora: {recordsNumber} del microservicio {nombreMicroservicio}'
        await send_telegram_message(message, chat_id_red)
    else:
        message = f'Sin alerta. El tráfico no rebasa la cota superior del microservicio {nombreMicroservicio}.'
        await send_telegram_message(message, chat_id_green)
    if recordsNumber< minRecords and minRecords/recordsNumber < 2:
        message = f'Alerta media de cantidad de registros. Se registran menos de {minRecords} para la hora: {recordsNumber} del microservicio {nombreMicroservicio}'
        await send_telegram_message(message, chat_id_yellow)
    elif recordsNumber< minRecords and minRecords/recordsNumber >= 2:
        message = f'Alerta alta de cantidad de registros. Se registran menos de {minRecords} para la hora: {recordsNumber} del microservicio {nombreMicroservicio}'
        await send_telegram_message(message, chat_id_red)
    else:
        message = f'Sin alerta. El tráfico no rebasa la cota inferior del microservicio {nombreMicroservicio}.'
        await send_telegram_message(message, chat_id_green)
    
    #Alerta por inconsistencia entre mensaje 'carfaxUsaData' y columna 'alertas'
    carfaxAlerts = filteredAnalysis.proveErrorAlert(regularizationFrame)
    if carfaxAlerts == 2:
        message = f'Alerta media: Existen {carfaxAlerts} registros con error y sin alerta del microservicio {nombreMicroservicio}'
        await send_telegram_message(message, chat_id_yellow)
    elif carfaxAlerts > 2:
        message = f'Alerta alta: Existen {carfaxAlerts} registros con error y sin alerta del microservicio {nombreMicroservicio}'
        await send_telegram_message(message, chat_id_red)
    else:
        message = f'Sin errores entre mensajes carfaxUsaData y Alertas del microservicio {nombreMicroservicio}'
        await send_telegram_message(message, chat_id_green)
    
    #Alerta por VINs que contengan alerta distinta o no contengan a la vez
    differentResponses = filteredAnalysis.verifyInfoRepeatedVins(regularizationFrame)['differentResponses']
    responsesKeys = differentResponses.keys()
    
    if len(responsesKeys) == 1:
        message = f'Alerta media: Existe VIN con distintos valores para una consulta{responsesKeys[0]}:{differentResponses[responsesKeys[0]]} del microservicio {nombreMicroservicio}'
        await send_telegram_message(message, chat_id_yellow)
    elif len(responsesKeys)>1:
        message = f'Alerta alta: Existen VINs con distintos valores para una consulta {responsesKeys} del microservicio {nombreMicroservicio}'
        await send_telegram_message(message, chat_id_red)
    else:
        message = f'Sin alerta de incongruencias entre VINs repetidos del microservicio {nombreMicroservicio}'
    await send_telegram_message(message, chat_id_green)


# In[11]:



# In[15]:


def regularizationRun(func):
    asyncio.run(func())


# In[16]:


def schedule_every_n_minutes():
    asyncio.run(regularizationAnalysis())  # Run the first time
    schedule.every(int(os.getenv('DIFFERENCE_MINUTES'))).minutes.do(regularizationRun, regularizationAnalysis)


# In[28]:


schedule.every().day.at("18:07").do(schedule_every_n_minutes)
async def main_loop():
    while True:
        schedule.run_pending()
        await asyncio.sleep(1)


# In[30]:


asyncio.run(main_loop())


# In[ ]:




