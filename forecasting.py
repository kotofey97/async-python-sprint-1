import logging
import threading
import subprocess
from multiprocessing import cpu_count, Pool, Manager
from concurrent.futures import ThreadPoolExecutor

from api_client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES

logger = logging.getLogger(__name__)

TASK_TIMEOUT = 30

def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    count_workers = len(CITIES)
    logger.debug("Thread workers count %s", count_workers)
    with ThreadPoolExecutor(max_workers=count_workers) as executor:
        futures = executor.map(DataFetchingTask.fetch, CITIES.keys())
        data = list(futures)[:1]

    manager = Manager()
    # queue = multiprocessing.Queue()
    queue = manager.Queue()
    calculation = DataCalculationTask(queue=queue, hours_to_start=9, hours_to_finish=19)

    workers_cpu = cpu_count() - 1

    with Pool(processes=workers_cpu) as pool:
        calculation_tasks = pool.map_async(calculation.calculate, data)

        calculation_tasks.wait(TASK_TIMEOUT)
    print('dddddd', calculation_tasks.ready())


    # city_name = "MOSCOW"
    # data = DataFetchingTask.fetch(city_name)
    # print("HHHH", data[0].forecasts)
    # print("HHHH", len(data[0].forecasts.forecasts))
    # print("HHHH", data['forecasts'])



    # city_name = "MOSCOW"
    # ywAPI = YandexWeatherAPI()
    # resp = ywAPI.get_forecasting(city_name)
    # print(resp)
    # pass


if __name__ == "__main__":
    forecast_weather()
