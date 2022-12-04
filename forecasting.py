from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool, cpu_count

from api_client import YandexWeatherAPI
from tasks import (DataAggregationTask, DataAnalyzingTask, DataCalculationTask,
                   DataFetchingTask)
from utils import (CITIES, FILENAME, FINISH_HOUR, START_HOURS, get_logger,
                   get_queue)

logger = get_logger(__name__)


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    count_workers = len(CITIES)
    logger.debug("Thread workers count %s", count_workers)

    api_client = YandexWeatherAPI()
    fetching = DataFetchingTask(api_client=api_client)
    with ThreadPoolExecutor(max_workers=count_workers) as executor:
        futures = executor.map(fetching.fetch, CITIES.keys())
        data = list(futures)

    queue = get_queue()
    calculation = DataCalculationTask(
        queue=queue,
        time_from=START_HOURS,
        time_till=FINISH_HOUR,
    )
    aggregation = DataAggregationTask(queue=queue, filename=FILENAME)

    workers_cpu = cpu_count() - 1

    with Pool(processes=workers_cpu) as pool:
        tasks_timeout = len(CITIES)
        calculation_tasks = pool.map_async(calculation.calculate, data)
        aggregation_task = pool.apply_async(aggregation.aggregate)
        calculation_tasks.wait(timeout=tasks_timeout+30)
        queue.put(None)
        aggregation_task.wait()

    analyze = DataAnalyzingTask(filename=FILENAME)
    analyze.analyze()
    comfortables = analyze.comfortables
    logger.info("Confortable cities: %s", comfortables)


if __name__ == "__main__":
    forecast_weather()
