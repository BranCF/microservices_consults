import os
import telegram
import pandas as pd
from datetime import datetime
import asyncio
from dotenv import load_dotenv

load_dotenv()

bot = telegram.Bot(token=os.getenv("BOT_TOKEN"))
CHAT_ID_GREEN = os.getenv('CHAT_ID_GREEN')
CHAT_ID_YELLOW = os.getenv('CHAT_ID_YELLOW')
CHAT_ID_RED = os.getenv('CHAT_ID_RED')


async def send_telegram_message(message: str, chat_semaphore: str):
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
    """
    Class with every method for basic analysis of microservices
    """

    # Constructor
    def __init__(self, dataframe, microservice_name: str, mean_std_records: dict, analysis_dict: dict):
        self.dataframe = dataframe
        self.microservice = microservice_name
        self.mean_std_records = mean_std_records
        self.response_code_column = analysis_dict["response_code_column"]
        self.max_code_alerts = float(analysis_dict["max_code_alerts"])
        self.error_codes = int(analysis_dict["error_codes"])
        self.response_time_column = analysis_dict["response_time_column"]
        self.request_time_column = analysis_dict["request_time_column"]
        self.time_maximum = int(analysis_dict["time_maximum"])
        self.max_count_delay = float(analysis_dict["max_count_delay"])
        self.api_key = analysis_dict["api_key"]
        self.api_key_max = int(analysis_dict["api_key_max"])
        self.last_time_sub = analysis_dict["last_time_sub"]
        self.n_minutes = analysis_dict["range_minutes"]

    def analyse_microservice(self):
        """
        Analyze microservice using the methods of this class.
        """
        print("Analyzing microservice...")
        asyncio.run(self.http_codes_alerts())
        print("Analyzing microservice...")
        asyncio.run(self.delay_alerts())
        print("Analyzing microservice...")
        asyncio.run(self.api_keys_alerts())
        print("Analyzing microservice...")
        asyncio.run(self.counts_records())

    # Public
    async def http_codes_alerts(self):
        """
        Counts how many http error codes dataframe has
        """
        total_columns = len(self.dataframe[self.response_code_column])
        self.dataframe[self.response_code_column] = self.dataframe[self.response_code_column].astype(int)
        bad_codes = self.dataframe[self.dataframe[self.response_code_column] >= self.error_codes]
        bad_code_counts = bad_codes[self.response_code_column].value_counts()
        print(bad_code_counts.items())
        print(list(bad_code_counts))
        for code, count in bad_code_counts.items():
            print(f"{code}: {count}")
            if self.max_code_alerts < count / total_columns < self.max_code_alerts + 0.1:
                message = f'Alerta media {self.microservice} de errores HTTP con código {code} con {count} apariciones'
                await send_telegram_message(message, CHAT_ID_YELLOW)
            elif count / total_columns > self.max_code_alerts + 0.1:
                message = f'Alerta alta {self.microservice} de errores HTTP con código {code} con {count} apariciones'
                await send_telegram_message(message, CHAT_ID_RED)
            else:
                message = f'Sin errores {self.microservice} de código HTTP'
                await send_telegram_message(message, CHAT_ID_GREEN)

    # Public
    async def delay_alerts(self):
        """
        Counts how many slow response records dataframe has and determines if the count is normal
        """
        records_without_time = len(
            self.dataframe[self.dataframe[self.response_time_column] == 'xD'][self.response_time_column])
        records_with_time = len(
            self.dataframe[self.dataframe[self.response_time_column] != 'xD'][self.response_time_column])
        if records_without_time > 0:
            message = f'Alerta media {self.microservice}: {records_without_time} registros no tienen tiempo de respuesta'
            await send_telegram_message(message, CHAT_ID_YELLOW)

        dataframe = self.dataframe[(self.dataframe[self.response_time_column] != 'xD') & (
                self.dataframe[self.request_time_column] != 'xD')]

        time_frame = self.calculate_delta_time(self.response_time_column, self.request_time_column)

        time_max = time_frame['secondsDifference'].max()

        if time_max > self.time_maximum:

            dataframe['secondsDifference'] = time_frame['secondsDifference']
            slow_time = dataframe[dataframe['secondsDifference'] > self.time_maximum]
            slow_time_counts = slow_time['secondsDifference'].value_counts()
            count = int(slow_time_counts.sum())

            if self.max_count_delay < count / records_with_time < self.max_count_delay + 0.1:
                message = f'Alerta media {self.microservice} de lentitud: {count} registros lentos'
                await send_telegram_message(message, CHAT_ID_YELLOW)
            elif count / records_with_time > self.max_count_delay + 0.1:
                message = f'Alerta alta {self.microservice} de lentitud: {count} registros lentos'
                await send_telegram_message(message, CHAT_ID_RED)
        else:
            message = f'Sin alerta {self.microservice} de lentitud'
            await send_telegram_message(message, CHAT_ID_GREEN)

    # Public
    async def api_keys_alerts(self):
        """
        Counts how many different apiKey records dataframe has and determines if the count is normal
        """
        bad_api_keys = self.dataframe[self.dataframe['apiKey'] != self.api_key]

        bad_api_counts = bad_api_keys['apiKey'].value_counts()

        for self.api_key, count in bad_api_counts.items():
            if count == self.api_key_max:
                message = f'Alerta media {self.microservice} para la api Key {self.api_key} con {count} apariciones'
                await send_telegram_message(message, CHAT_ID_YELLOW)
            elif count > self.api_key_max:
                message = f'Alerta alta {self.microservice} para la apiKey {self.api_key} con {count} apariciones'
                await send_telegram_message(message, CHAT_ID_RED)

        if not list(bad_api_counts):
            message = f'Sin alerta {self.microservice} con la apiKey'
            await send_telegram_message(message, CHAT_ID_GREEN)

    async def counts_records(self):
        """
        Counts how many records dataframe has and determines if the count is normal
        """
        date_object = datetime.strptime(str(self.last_time_sub), "%Y-%m-%d %H:%M:%S")

        day_name = date_object.strftime("%A")
        hour = date_object.hour

        mean_std = self.mean_std_records[f'{day_name}_{hour}']

        std = mean_std['std']
        mean = mean_std['mean']

        n_minutes_proportion = self.n_minutes / 60

        if std / mean > 0.6:
            max_records = n_minutes_proportion * (mean + 3 * std)
            min_records = n_minutes_proportion * (mean - 3 * std)
        else:
            max_records = n_minutes_proportion * (mean + 2.5 * std)
            min_records = n_minutes_proportion * (mean - 2.5 * std)

        if min_records < 0:
            min_records = 0

        records_number = self.dataframe.shape[0]

        if records_number != 0:
            if records_number > max_records and records_number / max_records < 2:
                message = f'Alerta media {self.microservice} de cantidad de registros. Se registran más de {max_records} para la hora: {records_number}'
                await send_telegram_message(message, CHAT_ID_YELLOW)
            elif records_number / max_records >= 2:
                message = f'Alerta alta {self.microservice} de cantidad de registros. Se registran más de {max_records} para la hora: {records_number}'
                await send_telegram_message(message, CHAT_ID_RED)
            else:
                message = f'Sin alerta {self.microservice}. El tráfico no rebasa la cota superior: {records_number}'
                await send_telegram_message(message, CHAT_ID_GREEN)

            if records_number < min_records and min_records / records_number < 2:
                message = f'Alerta media {self.microservice} de cantidad de registros. Se registran menos de {min_records} para la hora: {records_number}'
                await send_telegram_message(message, CHAT_ID_YELLOW)
            elif min_records / records_number >= 2:
                message = f'Alerta alta {self.microservice} de cantidad de registros. Se registran menos de {min_records} para la hora: {records_number}'
                await send_telegram_message(message, CHAT_ID_RED)
            else:
                message = f'Sin alerta {self.microservice}. El tráfico no rebasa la cota inferior: {records_number}'
                await send_telegram_message(message, CHAT_ID_GREEN)

    # Private
    def calculate_delta_time(self, column_a, column_b):
        """
        Converts column_a and column_b in datetime type and makes the difference between both columns
        Args:
           column_a (string) : Minuend column
           column_b (string): Subtracting column
        Returns:
           responsePeriod (DataFrame): Returns a column named 'secondsDifference' of the seconds difference as a dataframe
        """
        response_time_type = pd.to_datetime(self.dataframe[column_a], format='%Y-%m-%d %H:%M:%S')
        request_time_type = pd.to_datetime(self.dataframe[column_b], format='%Y-%m-%d %H:%M:%S')
        response_period = (response_time_type - request_time_type).to_frame()
        response_period['secondsDifference'] = pd.to_timedelta(response_period[0]).dt.total_seconds()
        response_period.drop([0], axis='columns', inplace=True)

        return response_period
