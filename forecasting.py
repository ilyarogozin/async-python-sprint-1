import logging
import sys
from multiprocessing import get_context
from multiprocessing.queues import Queue

from tasks import (DataAggregationTask, DataAnalyzingTask, DataCalculationTask,
                   DataFetchingTask)
from utils import CITIES, YandexWeatherAPI


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    ywAPI = YandexWeatherAPI()
    queue = Queue(ctx=get_context('forkserver'))
    fetching_task = DataFetchingTask(ywAPI, CITIES)
    calculation_task = DataCalculationTask()
    aggregation_task = DataAggregationTask(queue, fetching_task, calculation_task)
    analyzing_task = DataAnalyzingTask(queue, aggregation_task)
    result = analyzing_task.get_analyzed_data()
    print(result)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format=('%(asctime)s [%(levelname)s] %(name)s,'
                ' line %(lineno)d, %(message)s'),
        handlers=[logging.StreamHandler(stream=sys.stdout),
                  logging.FileHandler(filename=__file__ + '.log')]
    )
    forecast_weather()
