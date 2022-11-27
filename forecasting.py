import logging
import threading
import subprocess
import multiprocessing
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

def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    count_workers = len(CITIES)
    logger.debug("Thread workers count %s", count_workers)
    with ThreadPoolExecutor(max_workers=count_workers) as executor:
        futures = executor.map(DataFetchingTask.fetch, CITIES.keys())
        data = list(futures)

    # city_name = "MOSCOW"
    # data = DataFetchingTask.fetch(city_name)
    print("HHHH", data[0].forecasts)
    # print("HHHH", data['forecasts'])



    # city_name = "MOSCOW"
    # ywAPI = YandexWeatherAPI()
    # resp = ywAPI.get_forecasting(city_name)
    # print(resp)
    # pass


if __name__ == "__main__":
    forecast_weather()
