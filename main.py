import os
import json

# Unique right api key of microservice
API_KEY = os.getenv('API_KEY_CC')

# Dictionary with average and standard deviations of records by day and hour
with open(os.getenv('MEAN_RECORDS_JSON_MONGO'), 'r') as meanRecordsByHour:
    MEAN_STD_RECORDS = json.load(meanRecordsByHour)

# LOCAL
USERNAME = os.getenv('USER_MONGO_CC')
PASSWORD = os.getenv('PASSWORD_MONGO_CC')
PORT = int(os.getenv('PORT_MONGO_CC'))
CA_CERT = os.getenv('CA_CERT_MONGO_CC')

# PRODUCTION
# PROTOCOL = os.getenv('PROTOCOL_MONGO_CC')
# AUTHENTICATION_MECHANISM = os.getenv('AUTHENTICATION_MECHANISM_MONGO_CC')
# NODE_TYPE_MONGO = os.getenv('NODE_TYPE_MONGO_CC')
# APP_NAME = os.getenv('APP_NAME_MONGO_CC')

# BOTH
HOST = os.getenv('HOST_MONGO_CC')
DATABASE = os.getenv('DATABASE_MONGO_CC')
COLLECTION = os.getenv('COLLECTION_MONGO_CC')
CLIENT_CERT = os.getenv('CLIENT_CERT_MONGO_CC')

COLUMNS_REQUIRED = {column: 1 for column in ["vin", "responseBody","responseTime", "requestTime", "responseCode", "apiKey", "_id"]}
COLUMNS_REQUIRED["_id"]=0
TIME_TO_SEARCH = int(os.getenv('TIME_TO_SEARCH_CC'))
CONSULTA_CIUDADANA = 'CONSULTA CIUDADANA'