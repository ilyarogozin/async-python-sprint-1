import json
import logging
from multiprocessing import Pool, cpu_count
from multiprocessing.queues import Queue
from typing import Callable


class DataAggregationTask:
    def __init__(self, queue: Queue, calculation_func: Callable):
        self.queue: Queue = queue
        self.calculation_func: Callable = calculation_func
        self.buffer_for_result: list = []

    def _callback(self, result: list) -> None:
        self.buffer_for_result.append(result)
        logging.info(f'End of city data calculation: {result["city"]}')

    def _error_callback(self, error: Exception) -> Exception:
        logging.exception(f'Data calculation error: {error}')
        raise error

    def get_data_aggregation(self) -> None:
        cores_count: int = cpu_count()
        logging.info('Start of data aggregation')
        try:
            with Pool(processes=cores_count - 1) as pool:
                while city_data := self.queue.get(block=True, timeout=0):
                    city_name = city_data['geo_object']['locality']['name']
                    logging.info(f'Start calculating city data: {city_name}')
                    result = pool.apply_async(
                        self.calculation_func,
                        (city_data,),
                        callback=self._callback,
                        error_callback=self._error_callback
                    )
                    calculated_data = result.get()
                    self.queue.put(calculated_data)
                self.queue.put(None)
            with open('cities_calculated_data', 'w+') as f:
                json.dump(self.buffer_for_result, f, indent=1, sort_keys=True)
        except Exception as error:
            logging.exception(f'Error while aggregating data: {error}')
        logging.info('End of data aggregation')


class DataAnalyzingTask:
    def __init__(self, queue: Queue):
        self.queue: Queue = queue

    def get_analyzed_data(self) -> str:
        data: list = []
        logging.info('Start data analyzing')
        try:
            while city_calculated_data := self.queue.get(block=True, timeout=0):
                data.append(city_calculated_data)
            data.sort(
                key=lambda x: (
                    x['average rainless hours for all days'],
                    x['average temp for all days']
                ),
                reverse=True
            )
            rated_cities: list = []
            with open('cities_rating', 'w') as f:
                best_params: list = []
                best_cities: list = []
                for rating, city in enumerate(data, start=1):
                    if rating == 1:
                        best_params = [
                            city['average rainless hours for all days'],
                            city['average temp for all days']
                        ]
                        best_cities.append(city['city'])
                    else:
                        params: list = [
                            city['average rainless hours for all days'],
                            city['average temp for all days']
                        ]
                        if params == best_params:
                            best_cities.append(city['city'])
                    city.pop('days')
                    city['rating'] = rating
                    rated_cities.append(city)
                json.dump(rated_cities, f, indent=1, sort_keys=True)
        except Exception as error:
            logging.exception(f'Data analyzing error: {error}')
            raise error
        logging.info('End of data analyzing')
        return ('Наиболее благоприятные города,\n'
                'в которых средняя температура за всё время была самой высокой,\n'
                f'а количество времени без осадков — максимальным: {", ".join(best_cities)}')
