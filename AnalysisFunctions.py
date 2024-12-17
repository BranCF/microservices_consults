import os
import telegram
import pandas as pd
from datetime import datetime, timedelta

bot = telegram.Bot(token=os.getenv("BOT_TOKEN"))
CHAT_ID_GREEN = os.getenv('CHAT_ID_GREEN')
CHAT_ID_YELLOW = os.getenv('CHAT_ID_YELLOW')
CHAT_ID_RED = os.getenv('CHAT_ID_RED')

async def send_telegram_message(message, chat_semaphore):
    """
    Send an alert message (async).
    Args:
        message (str): Message to send
        chat_semaphore (str): Telegram id of chat to send the message
    """
    try:
        await bot.send_message(chat_id=chat_semaphore, text=message)
        print(f"Mensaje enviado a Telegram: {message}")
    except Exception as e:
        print(f"Error al enviar mensaje a Telegram: {e}")


class AnalysisMicroservice:

    #Constructor
    def __init__(self, dataframe, microservice_name, mean_std_records):
        self.dataframe = dataframe
        self.microservice = microservice_name
        self.mean_std_records = mean_std_records

    #Public
    async def http_codes_alerts(self, response_code_column, max_code_alerts, error_codes):
        """
        Counts how many http error codes dataframe has
        Args:
            response_code_column (str): the column name of the response codes
            max_code_alerts: Max number of allowed alerts
            error_codes: Definition of error codes
        """
        total_columns = len(self.dataframe[response_code_column])
        self.dataframe[response_code_column] = self.dataframe[response_code_column].astype(int)
        bad_codes = self.dataframe[self.dataframe[response_code_column] >= error_codes]
        bad_code_counts = bad_codes[response_code_column].value_counts()


        for code, count in bad_code_counts.items():
            if count == max_code_alerts:
                message = f'Alerta media {self.microservice} de errores HTTP con código {code} con {count} apariciones'
                await send_telegram_message(message, CHAT_ID_YELLOW)
            elif count > max_code_alerts:
                message = f'Alerta alta {self.microservice} de errores HTTP con código {code} con {count} apariciones'
                await send_telegram_message(message, CHAT_ID_RED)

            if not list(bad_code_counts):
                message = f'Sin errores {self.microservice} de código HTTP'
                await send_telegram_message(message, CHAT_ID_GREEN)

    #Private
    def calculate_delta_time(self, column_a, column_b):
        """
        Converts column_a and column_b in datetime type and makes the difference between both columns
        Args:
           column_a (string) : Minuend column
           column_b (string): Substrate column
        Returns:
           responsePeriod (DataFrame): Returns a column named 'secondsDifference' of the seconds difference as a dataframe
        """
        response_time_type = pd.to_datetime(self.dataframe[column_a], format='%Y-%m-%d %H:%M:%S')
        request_time_type = pd.to_datetime(self.dataframe[column_b], format='%Y-%m-%d %H:%M:%S')
        response_period = (response_time_type - request_time_type).to_frame()
        response_period['secondsDifference'] = pd.to_timedelta(response_period[0]).dt.total_seconds()
        response_period.drop([0], axis='columns', inplace=True)

        return response_period

    #Public
    async def delay_alerts(self, response_time_column, request_time_column, time_maximum, max_count_delay):
        """
        Counts how many slow response records dataframe has and determines if the count is normal
        Args:
            response_time_column (str): the column name of the response times
            request_time_column (str): the column name of the request times
            time_maximum (float): Maximum number of seconds of delay allowed
            max_count_delay (int): Max number of allowed slow records
        """
        records_without_time = len(self.dataframe[self.dataframe[response_time_column] is None][response_time_column])
        if records_without_time>0:
            message = f'Alerta media {self.microservice}: {records_without_time} registros no tienen tiempo de respuesta'
            await send_telegram_message(message, CHAT_ID_YELLOW)

        dataframe = self.dataframe[(self.dataframe[response_time_column] is not None) & (
                self.dataframe[request_time_column] is not None)]

        time_frame = self.calculate_delta_time(response_time_column, request_time_column)

        time_max = time_frame['secondsDifference'].max()

        if time_max > time_maximum:

            dataframe['secondsDifference'] = time_frame['secondsDifference']
            slow_time = dataframe[dataframe['secondsDifference'] > time_maximum]
            slow_time_counts = slow_time['secondsDifference'].value_counts()
            count = int(slow_time_counts.sum())

            if count == max_count_delay:
                message = f'Alerta media {self.microservice} de lentitud: {count} registros lentos'
                await send_telegram_message(message, CHAT_ID_YELLOW)
            elif count > max_count_delay:
                message = f'Alerta alta {self.microservice} de lentitud: {count} registros lentos'
                await send_telegram_message(message, CHAT_ID_RED)
        else:
            message = f'Sin alerta {self.microservice} de lentitud'
            await send_telegram_message(message, CHAT_ID_GREEN)

    #Public
    async def api_keys_alerts(self, api_key_max, api_key):
        """
        Counts how many different apiKey records dataframe has and determines if the count is normal
        Args:
            api_key_max (int): Max number of allowed different apiKey records
            api_key(string): allowed apiKey
        """

        bad_api_keys = self.dataframe[self.dataframe['apiKey'] != api_key]

        bad_api_counts = bad_api_keys['apiKey'].value_counts()

        for api_key, count in bad_api_counts.items():
            if count == api_key_max:
                message = f'Alerta media {self.microservice} para la api Key {api_key} con {count} apariciones'
                await send_telegram_message(message, CHAT_ID_YELLOW)
            elif count > api_key_max:
                message = f'Alerta alta {self.microservice} para la apiKey {api_key} con {count} apariciones'
                await send_telegram_message(message, CHAT_ID_RED)

        if not list(bad_api_counts):
            message = f'Sin alerta {self.microservice} con la apiKey'
            await send_telegram_message(message, CHAT_ID_GREEN)

    async def counts_records(self, last_time_sub, n_minutes):
        """
        Counts how many records dataframe has and determines if the count is normal
        Args:
            last_time_sub (string): Day and hour of compare
            n_minutes: Difference between the older record and the newer record
        """

        date_object = datetime.strptime(str(last_time_sub), "%Y-%m-%d %H:%M:%S.%f")

        day_name = date_object.strftime("%A")
        hour = date_object.hour

        mean_std = self.mean_std_records[f'{day_name}_{hour}']

        std = mean_std['std']
        mean = mean_std['mean']

        n_minutes_proportion = n_minutes / 60

        if std / mean > 0.6:
            max_records = n_minutes_proportion * (mean + 2 * std)
            min_records = n_minutes_proportion * (mean - 2 * std)
        else:
            max_records = n_minutes_proportion * (mean + 1.5 * std)
            min_records = n_minutes_proportion * (mean - 1.5 * std)

        if min_records < 0:
            min_records = 0

        records_number = self.dataframe.shape[0]

        if records_number > max_records and records_number / max_records < 2:
            message = f'Alerta media {self.microservice} de cantidad de registros. Se registran más de {max_records} para la hora: {records_number}'
            await send_telegram_message(message, CHAT_ID_YELLOW)
        elif records_number / max_records >= 2:
            message = f'Alerta alta {self.microservice} de cantidad de registros. Se registran más de {max_records} para la hora: {records_number}'
            await send_telegram_message(message, CHAT_ID_RED)
        else:
            message = f'Sin alerta {self.microservice}. El tráfico no rebasa la cota superior.'
            await send_telegram_message(message, CHAT_ID_GREEN)

        if records_number < min_records and min_records / records_number < 2:
            message = f'Alerta media {self.microservice} de cantidad de registros. Se registran menos de {min_records} para la hora: {records_number}'
            await send_telegram_message(message, CHAT_ID_YELLOW)
        elif min_records / records_number >= 2:
            message = f'Alerta alta {self.microservice} de cantidad de registros. Se registran menos de {min_records} para la hora: {records_number}'
            await send_telegram_message(message, CHAT_ID_RED)
        else:
            message = f'Sin alerta {self.microservice}. El tráfico no rebasa la cota inferior.'
            await send_telegram_message(message, CHAT_ID_GREEN)