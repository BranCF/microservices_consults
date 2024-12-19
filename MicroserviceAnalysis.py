import json
import os
from AnalysisFunctions import AnalysisMicroservice
from ConnectionFunctions import connect_to_mongo, fetch_data_from_mongo, fetch_data_from_mysql, get_the_last_date
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

CC = {'connection': {'name': os.getenv('NAME_CC'),
                     'database_type': 'mongodb',
                     'protocol': os.getenv('PORT_MONGO_CC'),
                     'auth_mechanism': os.getenv('AUTH_MECHANISM_MONGO_CC'),
                     'node_type': os.getenv('NODE_TYPE_MONGO_CC'),
                     'app_name': os.getenv('APP_NAME_MONGO_CC'),
                     'username': os.getenv('USER_MONGO_CC'),  # local
                     'password': os.getenv('PASSWORD_MONGO_CC'),  # local
                     'port': int(os.getenv('PORT_MONGO_CC')),  # local
                     'host': os.getenv('HOST_MONGO_CC'),
                     'ca_cert': os.getenv('CA_CERT_MONGO_CC'),
                     'client_cert': os.getenv('CLIENT_CERT_MONGO_CC'),
                     'database': os.getenv('DATABASE_MONGO_CC'),
                     'collection': os.getenv('COLLECTION_MONGO_CC')
                     },
      'analysis': {'range_minutes': int(os.getenv('RANGE_MINUTES_CC')),
                   'mean_std_records': os.getenv('MEAN_RECORDS_JSON_CC'),
                   'response_code_column': os.getenv('RESPONSE_CODE_CC'),
                   'response_time_column': os.getenv('RESPONSE_TIME_CC'),
                   'request_time_column': os.getenv('REQUEST_TIME_CC'),
                   'max_code_alerts': os.getenv('MAX_CODE_CC'),
                   'error_codes': os.getenv('ERROR_CODES_CC'),
                   'time_maximum': os.getenv('MAXTIME_CC'),
                   'max_count_delay': os.getenv('MAX_DELAY_CC'),
                   'api_key': os.getenv('API_KEY_CC'),
                   'api_key_max': os.getenv('API_MAX_CC'),
                   }
      }

REGULARIZATION = {'name': os.getenv('NAME_REGULARIZATION'),
                  'connection': {'database_type': 'mysql',
                                 'db_config': {
                                     'host': os.getenv('HOST_REGULARIZATION'),
                                     'port': int(
                                         os.getenv('PORT_REGULARIZATION')),
                                     'user': os.getenv(
                                         'USER_MYSQL_REGULARIZATION'),
                                     'password': os.getenv(
                                         'PASSWORD_REGULARIZATION'),
                                     'database': os.getenv(
                                         'DATABASE_REGULARIZATION'),
                                     'ssl': {'ssl_ca': os.getenv(
                                         'CA_CERTIFICATE_REGULARIZATION')}
                                 },
                                 'view': os.getenv('VIEW_NAME_REGULARIZATION')
                                 },
                  'analysis': {'range_minutes': int(os.getenv('RANGE_MINUTES_REGULARIZATION')),
                               'mean_std_records': os.getenv('MEAN_RECORDS_JSON_REGULARIZATION'),
                               'response_code_column': os.getenv('RESPONSE_CODE_REGULARIZATION'),
                               'response_time_column': os.getenv('RESPONSE_TIME_REGULARIZATION'),
                               'request_time_column': os.getenv('REQUEST_TIME_REGULARIZATION'),
                               'max_code_alerts': os.getenv('MAX_CODE_REGULARIZATION'),
                               'error_codes': os.getenv('ERROR_CODES_REGULARIZATION'),
                               'time_maximum': os.getenv('MAXTIME_REGULARIZATION'),
                               'max_count_delay': os.getenv('MAX_DELAY_REGULARIZATION'),
                               'api_key': os.getenv('API_KEY_REGULARIZATION'),
                               'api_key_max': os.getenv('API_MAX_REGULARIZATION'),
                               }
                  }
MELI = {'name': os.getenv('NAME_MELI'),
        'connection': {'database_type': 'mongodb',
                       'protocol': os.getenv('PROTOCOL_MONGO_MELI'),
                       'auth_mechanism': os.getenv('AUTH_MECHANISM_MONGO_MELI'),
                       'node_type': os.getenv('NODE_TYPE_MONGO_MELI'),
                       'app_name': os.getenv('APP_NAME_MONGO_MELI'),
                       'username': os.getenv('USER_MONGO_MELI'),  # local
                       'password': os.getenv('PASSWORD_MONGO_MELI'),  # local
                       'port': int(os.getenv('PORT_MONGO_MELI')),  # local
                       'host': os.getenv('HOST_MONGO_MELI'),
                       'ca_cert': os.getenv('CA_CERT_MONGO_MELI'),
                       'client_cert': os.getenv('CLIENT_CERT_MONGO_MELI'),
                       'database': os.getenv('DATABASE_MONGO_MELI'),
                       'collection': os.getenv('COLLECTION_MONGO_MELI')
                       },
        'analysis': {'range_minutes': int(os.getenv('RANGE_MINUTES_MELI')),
                     'mean_std_records': os.getenv('MEAN_RECORDS_JSON_MELI'),
                     'response_code_column': os.getenv('RESPONSE_CODE_MELI'),
                     'response_time_column': os.getenv('RESPONSE_TIME_MELI'),
                     'request_time_column': os.getenv('REQUEST_TIME_MELI'),
                     'max_code_alerts': os.getenv('MAX_CODE_MELI'),
                     'error_codes': os.getenv('ERROR_CODES_MELI'),
                     'time_maximum': os.getenv('MAXTIME_MELI'),
                     'max_count_delay': os.getenv('MAX_DELAY_MELI'),
                     'api_key': os.getenv('API_KEY_MELI'),
                     'api_key_max': os.getenv('API_MAX_MELI'),
                     }
        }


def analyses_microservice(microservice: dict):
    """
    Analyze a microservice with use of classes and connection functions
    Args:
        microservice (dict): Dictionary with the necessary connection and analysis variables
    """
    # PROD
    # end_time = datetime.now()
    # start_time = end_time - timedelta(minutes=microservice['analysis']['range_minutes'])

    if microservice['connection']['database_type'] == 'mongodb':
        collection = connect_to_mongo(microservice['connection']['username'], microservice['connection']['password'],
                                      microservice['connection']['host'],
                                      microservice['connection']['port'], microservice['connection']['ca_cert'],
                                      microservice['connection']['database'], microservice['connection']['client_cert'],
                                      microservice['connection']['collection'])  # local

        # local
        end_time = collection.find_one({}, {microservice['analysis']['response_time_column']: 1},
                                       sort=[(microservice['analysis']['response_time_column'], -1)])
        start_time = end_time - timedelta(minutes=microservice['analysis']['range_minutes'])

        data = fetch_data_from_mongo(collection, {}, microservice['analysis']['response_time_column'], start_time,
                                     end_time)
        df = pd.json_normalize(data)
    elif microservice['connection']['database_type'] == 'mysql':
        # local
        end_time = list(get_the_last_date(microservice['connection']['db_config'], microservice['connection']['view'],
                                          microservice['analysis']['response_time_column']))[
                       12] + timedelta(hours=6)
        start_time = end_time - timedelta(minutes=microservice['analysis']['range_minutes'])

        data = fetch_data_from_mysql(microservice['connection']['db_config'], microservice['connection']['view'],
                                     start_time - timedelta(hours=6), end_time - timedelta(hours=6),
                                     microservice['analysis']['response_time_column'])
        df = pd.DataFrame(data, columns=['VIN', '_id', 'host', 'ip', 'apiKey', 'userId', 'idReporte',
                                         'requestParameters', 'requestUrl', 'responseCode', 'responseCodeStatus',
                                         'requestTime', 'responseTime', 'labels', 'idRespuesta', 'idConsulta',
                                         'responseBody', 'carfaxUsaData', 'alertas', 'firewallUsa'])

    with open(microservice['analysis']['mean_std_records'], 'r') as meanRecordsByHour:
        mean_std_records = json.load(meanRecordsByHour)

    microservice['analysis']['last_time_sub'] = end_time

    df = df.replace(np.nan, 'xD')

    microservice_analyzed = AnalysisMicroservice(df, microservice['name'], mean_std_records, microservice['analysis'])

    microservice_analyzed.analyse_microservice()


analyses_microservice(REGULARIZATION)
