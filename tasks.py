import json
import logging
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool, cpu_count
from multiprocessing.queues import Queue

from utils import YandexWeatherAPI


class DataFetchingTask:
    def __init__(self, ywAPI: YandexWeatherAPI, cities: dict):
        self.cities: dict = cities
        self.api: YandexWeatherAPI = ywAPI

    def get_data_fetching(self) -> list:
        logging.info('Начало получения данных по API')
        try:
            with ThreadPoolExecutor(max_workers=10) as pool:
                result: list = list(pool.map(self.api.get_forecasting, self.cities.keys()))
        except Exception as error:
            logging.exception(f'Ошибка при запросе к API: {error}')
            raise error
        logging.info('Завершение получения данных по API')
        return result


class DataCalculationTask():
    def __init__(self):
        self.rainless_conditions: list = [
            'clear',
            'partly-cloudy',
            'cloudy',
            'overcast',
            'drizzle'
        ]

    def get_data_calculation(self, city: dict) -> list:
        result: list = list()
        city_name = city['geo_object']['locality']['name']
        logging.info(f'Начало вычисления данных по городу: {city_name}')
        try:
            result.append({'city': city_name})
            result[-1]['days'] = list()
            num_of_days: int = 0
            sum_avg_temps_for_all_days: float = 0.0
            sum_rainless_hours_for_all_days: int = 0
            for forecast in city['forecasts']:
                result[-1]['days'].append({'date': forecast['date']})
                rainless_hours: int = 0
                sum_temp_per_day: float = 0.0
                num_of_hours: int = 0
                for hour in forecast['hours']:
                    if 9 <= int(hour['hour']) <= 19:
                        num_of_hours += 1
                        sum_temp_per_day += hour['temp']
                        if hour['condition'] in self.rainless_conditions:
                            rainless_hours += 1
                if num_of_hours == 11:
                    num_of_days += 1
                    result[-1]['days'][-1]['rainless hours'] = rainless_hours
                    avg_temp_per_day: float = round(sum_temp_per_day / 11, 1)
                    sum_avg_temps_for_all_days += avg_temp_per_day
                    sum_rainless_hours_for_all_days += rainless_hours
                    result[-1]['days'][-1]['averange temperature per day'] = avg_temp_per_day
            result[-1]['average temp for all days'] = round(sum_avg_temps_for_all_days / num_of_days, 1)
            result[-1]['average rainless hours for all days'] = round(sum_rainless_hours_for_all_days / num_of_days, 1)
        except Exception as error:
            logging.exception(f'Ошибка при вычислении данных: {error}')
            raise error
        logging.info(f'Завершение вычисления данных по городу: {city_name}')
        return result


class DataAggregationTask:
    def __init__(self, queue: Queue, fetching_task, calculation_task):
        self.queue: Queue = queue
        self.fetching_task = fetching_task
        self.calculation_task = calculation_task

    def _callback(self, result: list) -> None:
        with open('cities_calculated_data', "w") as f:
            json.dump(result, f, indent=1, sort_keys=True)
        logging.info('Завершение аггрегации данных')

    def _error_callback(self, error: Exception) -> Exception:
        logging.exception(f'Ошибка при аггрегации данных: {error}')
        raise error

    def get_data_aggregation(self) -> None:
        cores_count: int = cpu_count()
        cities: list = self.fetching_task.get_data_fetching()
        logging.info('Начало аггрегации данных')
        with Pool(processes=cores_count - 1) as pool:
            result = pool.map_async(
                self.calculation_task.get_data_calculation,
                cities,
                callback=self._callback,
                error_callback=self._error_callback
            )
            result.wait()
            for calculated_data in result.get():
                self.queue.put(calculated_data)
            self.queue.put(None)


class DataAnalyzingTask:
    def __init__(self, queue: Queue, aggregation_task):
        self.queue: Queue = queue
        self.aggregation_task = aggregation_task

    def get_analyzed_data(self) -> str:
        self.aggregation_task.get_data_aggregation()
        data: list = []
        logging.info('Начало анализа данных')
        try:
            while city_calculated_data := self.queue.get():
                data.append(city_calculated_data)
            data.sort(
                key=lambda x: (
                    x[0]['average rainless hours for all days'],
                    x[0]['average temp for all days']
                ),
                reverse=True
            )
            with open('cities_rating', "w") as f:
                for rating, city in enumerate(data, start=1):
                    city[0].pop('days')
                    city[0]['rating'] = rating
                    json.dump(city, f, indent=1, sort_keys=True)
        except Exception as error:
            logging.exception(f'Ошибка при анализе данных: {error}')
            raise error
        logging.info('Завершение анализа данных')
        return ('Наиболее благоприятный город,\n'
                'в котором средняя температура за всё время была самой высокой,\n'
                f'а количество времени без осадков — максимальным: {data[0][0]["city"]}')
