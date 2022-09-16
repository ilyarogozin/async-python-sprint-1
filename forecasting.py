import logging
import sys
from multiprocessing import get_context
from multiprocessing.queues import Queue

from data_extract import DataFetchingTask
from data_load import DataAggregationTask, DataAnalyzingTask
from data_transform import DataCalculationTask
from utils import CITIES, YandexWeatherAPI


def forecast_weather() -> None:
    """
    Анализ погодных условий по городам
    """
    ywAPI = YandexWeatherAPI()
    queue = Queue(ctx=get_context('forkserver'))
    fetching_task = DataFetchingTask(ywAPI, CITIES, queue)
    calculation_task = DataCalculationTask()
    aggregation_task = DataAggregationTask(queue, calculation_task.get_data_calculation)
    analyzing_task = DataAnalyzingTask(queue)
    fetching_task.get_data_fetching()
    aggregation_task.get_data_aggregation()
    result = analyzing_task.get_analyzed_data()
    logging.info(result)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format=('%(asctime)s [%(levelname)s] %(name)s,'
                ' %(filename)s, line %(lineno)d, %(message)s'),
        handlers=[logging.StreamHandler(stream=sys.stdout),
                  logging.FileHandler(filename=__file__ + '.log')]
    )
    forecast_weather()
