from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import pymysql.cursors


# PRODUCTION
# def connect_to_mongo(protocol, host, authentication_mechanism, node_type, app_name, database, collection, client_certificate):
#     """
#     Establishes the connection to MongoDB and returns the client and the collection.
#     Args:
#         protocol (str): mongodb or mongodb+srv
#         host (str): Host name
#         authentication_mechanism (str): Mongo authentication mechanism to use
#         node_type (str): ELECTABLE, ANALYTICS OR READ_ONLY
#         app_name (str): name of application to connect to
#         database (str): name of database to connect to
#         collection (str): name of collection to connect to
#         client_certificate (str): Path to client certificate file
#     Returns:
#         collection connected to mongodb
#    """
#     try:
#         client = MongoClient(
#             host=f"{protocol}://{host}/?authSource=%24external&authMechanism={authentication_mechanism}&retryWrites=true&readPreference=secondary&readPreferenceTags=nodeType:{node_type}&w=majority&appName={app_name}",
#             tls=True,
#             tlsCertificateKeyFile=client_certificate,
#             serverSelectionTimeoutMS=5000,
#             socketTimeoutMS=40000,
#             connectTimeoutMS=300000
#         )
#         db = client[database]
#         collection = db[collection]
#         return collection
#     except ConnectionFailure as e:
#         print("Failed to connect to MongoDB:", e)
#         return None

# LOCAL
def connect_to_mongo(username, password, host, port, ca_cert, client_cert, database, collection):
    """
    Establishes the connection to MongoDB and returns the client and the collection.
    """
    try:
        client = MongoClient(
            host=f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin",
            tls=True,
            tlsCAFile=ca_cert,
            tlsCertificateKeyFile=client_cert
        )
        db = client[database]
        collection = db[collection]
        print(collection)
        return collection
    except ConnectionFailure as e:
        print("Failed to connect to MongoDB:", e)
        return None


def fetch_data_from_mongo(collection, required_fields, response_time_column, start_time, end_time):
    """
   Extracts data from MongoDB by applying a filter based on the last 10 minutes from the current computer time, selecting the necessary columns.
   Args:
       collection (Mongo Collection): Collection to extract data from a mongo database
       required_fields (dict): Dictionary of required fields to extract data from database as keys
       response_time_column(str): Column name of the response time column
       end_time(datetime): Finish date and time to extract data from database
       start_time(datetime): Start date and time to extract data from database
    Returns:
        documents (dict): List of dictionaries of data extracted from the mongo database
   """

    try:
        query = {response_time_column: {"$gte": start_time, "$lte": end_time}}
        q = collection.find(query, required_fields)
        documents = list(q)
        return documents
    except Exception as e:
        print(f"Error al extraer datos desde MongoDB: {e}")
        return []


def connect_to_mysql(db_config):
    """
    Connects to a MySQL database
    Args:
        db_config (dict): Dictionary with user, password, host, port, database name and ssl certs and keys to connect
    Returns:
      conn: pymsql connection object
    """
    try:
        conn = pymysql.connect(**db_config)
        return conn
    except pymysql.Error as err:
        return err


def disconnect_to_mysql(conn, cursor):
    """
    Disconnects to a database
    Args:
        conn
        cursor
    Returns:
    """
    cursor.close()
    conn.close()


def fetch_data_from_mysql(db_config: dict, view: str, last_timesub, last_time, response_time_column: str):
    """
    Searches the rows of a MySQL view where its response time column values are between lastTime and lastTimeSub
    Args:
        db_config (dict): Connection dictionary
        view (string): View's name
        last_timesub: The older date
        last_time : The newer date
    Returns:
        result (tuple): The rows mentioned
    """
    try:
        conn = connect_to_mysql(db_config)
        cursor = conn.cursor()
        # query = f"SELECT * FROM {view} WHERE STR_TO_DATE(responseTime, '%Y-%m-%d %H:%i:%s') BETWEEN STR_TO_DATE('{lastTimeSub}', '%Y-%m-%d %H:%i:%s') AND STR_TO_DATE('{lastTime}', '%Y-%m-%d %H:%i:%s') ORDER BY STR_TO_DATE(responseTime, '%Y-%m-%d %H:%i:%s') DESC;"
        query = f"SELECT * FROM {view} WHERE {response_time_column} BETWEEN '{last_timesub.strftime('%Y-%m-%d %H:%M:%S')}' AND '{last_time.strftime('%Y-%m-%d %H:%M:%S')}' ORDER BY {response_time_column} DESC;"
        print(query)
        cursor.execute(query)
        result = cursor.fetchall()
        disconnect_to_mysql(conn, cursor)
        return result
    except AttributeError as atr_err:
        return atr_err


def get_the_last_date(db_config: dict, view: str, response_time_column: str):
    '''
    Searches the first row of a MySQL view where its response time column value is the most recent
    Args:
        db_config (dict): Connection dictionary
        view (string): View's name
        response_time_column(str): Column name of the response time column
    Returns:
        result (tuple): The row mentioned
    '''
    try:
        conn = connect_to_mysql(db_config)
        cursor = conn.cursor()
        # query = f"SELECT * FROM {view} ORDER BY STR_TO_DATE(responseTime, '%Y-%m-%d %H:%i:%s') DESC;"
        query = f"SELECT * FROM {view} ORDER BY {response_time_column} DESC;"
        print(query)
        cursor.execute(query)
        result = cursor.fetchone()
        disconnect_to_mysql(conn, cursor)
        return result
    except AttributeError as atr_err:
        return atr_err
