import json
import os
from AnalysisFunctions import AnalysisMicroservice
from ConnectionFunctions import connect_to_mongo, fetch_data_from_mongo
import pandas as pd




def analyses_microservice(connection_dict:dict, analysis_dict:dict):
    """
    Send an alert message (async).
    Args:
        message (str): Message to send
        chat_semaphore (str): Telegram id of chat to send the message
    """
    connect_to_mongo(connection_dict['username'], connection_dict['password'], connection_dict['host'], connection_dict['port'], connection_dict['database'], connection_dict['collection'])
    microservice = AnalysisMicroservice()
