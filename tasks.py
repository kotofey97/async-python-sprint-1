import csv
import operator
import os
import sys
from multiprocessing import Queue
from typing import Any, List, Optional, Tuple

from models import CityModel
from utils import GOOD_WEATHER, get_logger

logger = get_logger(__name__)


class DataFetchingTask:
    """Получение данных через API."""

    def __init__(self, api_client):
        self.api_client = api_client

    def fetch(self, city: str) -> CityModel:
        result = self.api_client.get_forecasting(city)
        logger.debug(f"API request for city:{city}")
        return CityModel(city=city, forecasts=result)


class DataCalculationTask:
    """Вычисление погодных параметров."""

    def __init__(self, queue: Queue, time_from: int, time_till: int):
        self.queue: Queue = queue
        self.time_from: int = time_from
        self.time_till: int = time_till

    def calculate(self, data: 'CityModel'):
        """Формирует результат для города."""
        forecasts = data.forecasts.dict()
        city_name = data.city
        stat = self.get_stat(forecasts)
        stat['city'] = city_name
        logger.debug(
            "DataCalculationTask result for city %s add in queue",
            city_name,
        )
        self.queue.put(stat)
        return stat

    def get_stat(self, forecasts: dict) -> dict:
        """
        Вычисление средних значений для города.

        {
            'temp_avg': 13.82,
            'clear_avg': 1.25,
            'dates': {
                '2022-05-26': {
                    'day_temp_avg': 18.0,
                    'hours_clear': 4,
                },
                ...
            },
        }
        """
        daily_averages: dict = {'dates': {}}
        dates = daily_averages['dates']
        temp_avg, clear_avg, day_count = 0, 0, 0
        for day_data in forecasts['forecasts']:
            day_temp_avg: Optional[float] = 0
            hours_count, hours_clear = 0, 0
            if len(day_data['hours']):
                for hour_data in day_data['hours']:
                    if self.time_from <= hour_data['hour'] < self.time_till:
                        day_temp_avg += hour_data['temp']
                        hours_count += 1
                        if hour_data['condition'] in GOOD_WEATHER:
                            hours_clear += 1
                day_temp_avg = self.get_avg(
                    day_temp_avg, hours_count, 1
                ) if hours_count else None
                if day_temp_avg:
                    temp_avg += day_temp_avg
                clear_avg += hours_clear
                day_count += 1
                day_stat = {
                    'day_temp_avg': day_temp_avg,
                    'hours_clear': hours_clear,
                }
            else:
                day_stat = {'day_temp_avg': None, 'hours_clear': None}
            dates[day_data['date']] = day_stat

        temp_avg = self.get_avg(
            temp_avg, day_count, 2,
        ) if temp_avg and day_count else None
        clear_avg = self.get_avg(
            clear_avg, day_count, 2,
        ) if day_count else None
        daily_averages['temp_avg'] = temp_avg
        daily_averages['clear_avg'] = clear_avg
        return daily_averages

    @staticmethod
    def get_avg(sum, count, digits):
        return round((sum / count), digits)


class DataAggregationTask:
    """Объединение вычисленных данных."""

    def __init__(self, filename: str, queue: Queue):
        self.queue: Queue = queue
        self.filename: str = self.remove_file_if_exist(filename)

    def remove_file_if_exist(self, filename: str) -> str:
        """Удаляет файл, если такой уже был."""
        if os.path.exists(filename):
            os.remove(filename)
            logger.debug("Old file removed %s", filename)
        return filename

    def aggregate(self, data: Optional[dict] = None):
        """Соединение данных."""
        if data:
            return self.save_to_file(data=data)

        queue_item = self.queue.get()
        while queue_item:
            logger.debug(
                'DataCalculationTask result for city %s get from queue',
                queue_item['city'],
            )
            self.save_to_file(data=queue_item)
            queue_item = self.queue.get()

    def check_empty_file(self) -> bool:
        """Проверка пустой файл или нет."""
        return os.path.getsize(self.filename) == 0

    def save_to_file(self, data):
        """Сохранение данных в файл."""
        with open(self.filename, 'a+', newline='', encoding='utf-8') as file:
            dates_list = list(data['dates'].keys())
            data_writer = csv.writer(file)

            if self.check_empty_file():
                headers = ['Город/день', ''] + dates_list + ['Среднее']
                data_writer.writerow(headers)

            day_temp_avg_list = [
                data['dates'][temp]['day_temp_avg']
                if data['dates'][temp]['day_temp_avg'] else "---"
                for temp in dates_list
            ]
            first_row = [data['city'], 'Температура, среднее']
            first_row.extend(day_temp_avg_list)
            first_row.append(data['temp_avg'])

            hours_clear_list = [
                data['dates'][temp]['hours_clear']
                if data['dates'][temp]['hours_clear'] is not None else "---"
                for temp in dates_list
            ]
            second_row = ['', 'Без осадков, часов']
            second_row.extend(hours_clear_list)
            second_row.append(data['clear_avg'])

            data_writer.writerow(first_row)
            data_writer.writerow(second_row)


class DataAnalyzingTask:
    """Финальный анализ и получение результата."""

    def __init__(self, filename: str):
        self.filename: str = self.check_file_exist(filename)
        self.comfortables: List[str] = list()

    def check_file_exist(self, filename: str) -> str:
        """Проверка наличия файла."""
        if os.path.isfile(filename):
            return filename
        else:
            logger.error("File to analyze does'n exist: %s", filename)
            sys.exit(1)

    def analyze(self):
        """Анализ данных."""
        cities, ratings = self.get_cities_ratings()
        self.save_rating(ratings)
        self.set_comfortables(cities, ratings)

    def set_comfortables(self, cities, ratings):
        """Определение комфортных городов."""
        top = min(ratings)
        best_city = [
            idx for idx, rating in enumerate(ratings) if rating == top
        ]
        self.comfortables = operator.itemgetter(*best_city)(cities)

    def get_cities_ratings(self) -> Tuple[List[Any], List[int]]:
        """Получение рейтинга городов."""
        cities_list, ratings = [], []
        temp_list, clear_list = [], []
        with open(self.filename, encoding='utf-8') as file:
            file_reader = csv.reader(file, delimiter=",")
            count_row = 0
            for row in file_reader:
                if count_row % 2:
                    cities_list.append(row[0])
                    temp_list.append(float(row[-1]))
                elif not count_row % 2 and count_row > 0:
                    clear_list.append(float(row[-1]))
                count_row += 1

        data_to_rank = list(zip(temp_list, clear_list))
        data_sorted = sorted(
            data_to_rank,
            key=lambda item: item[0] + item[1],
            reverse=True
        )
        ratings = [data_sorted.index(idx) + 1 for idx in data_to_rank]
        return cities_list, ratings

    def save_rating(self, rating):
        """Добавление столбика с рейтингом"""
        file_data = []
        with open(self.filename, mode='r', encoding='utf-8') as file:
            file_reader = csv.reader(file, delimiter=",")
            for row in file_reader:
                file_data.append(row)

        with open(self.filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            count_row = 0
            rate_ind = 0
            for row in file_data:
                if count_row == 0:
                    row.append('Рейтинг')
                elif count_row % 2:
                    row.append(rating[rate_ind])
                    rate_ind += 1
                count_row += 1
                writer.writerow(row)
