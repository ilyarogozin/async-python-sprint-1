import logging
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.queues import Queue
from typing import Iterator

from utils import YandexWeatherAPI


class DataFetchingTask:
    def __init__(self, ywAPI: YandexWeatherAPI, cities: dict, queue: Queue):
        self.cities: dict = cities
        self.api: YandexWeatherAPI = ywAPI
        self.queue: Queue = queue

    def get_data_fetching(self):
        logging.info('Start receiving data via API')
        try:
            with ThreadPoolExecutor() as pool:
                fetched_data: Iterator = pool.map(self.api.get_forecasting, self.cities.keys())
                for city in fetched_data:
                    self.queue.put(city)
                self.queue.put(None)
        except Exception as error:
            logging.exception(f'API request error: {error}')
            raise error
        logging.info('End of receiving data via API')
