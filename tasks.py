import operator
import os
import sys
from typing import Any, List, Tuple

import pandas as pd
from pandas import DataFrame as df
from pandas import concat, read_csv

from api_client import YandexWeatherAPI
from models import CityModel, DataCalculationResult
from utils import get_logger

logger = get_logger(__name__)


class DataFetchingTask:
    """Получение данных через API."""

    @staticmethod
    def fetch(city: str) -> CityModel:
        result = YandexWeatherAPI().get_forecasting(city)
        logger.debug(f"API request for city:{city}")
        return CityModel(city=city, forecasts=result)


class DataCalculationTask:
    """Вычисление погодных параметров."""

    def __init__(self, queue, time_from, time_till) -> None:
        self.queue = queue
        self.time_from = time_from
        self.time_till = time_till

    def calculate(self, data):
        """Формирует результат для города."""
        forecasts = data.forecasts.dict()
        city_name = data.city
        daily_stat = self.daily_stat(forecasts)
        daily_stat.fillna("---", inplace=True)
        total_stat = self.total_stat(daily_stat)

        city = df([city_name, None], columns=["city"])
        result = DataCalculationResult(
            city=city, daily_averages=daily_stat, averages=total_stat
        )
        logger.debug(
            "DataCalculationResult for city %s add in queue",
            city_name,
        )
        self.queue.put(result)

    def daily_stat(self, forecasts):
        """Вычисление средних значений по дням."""
        daily_averages = df(
            columns=["day_temp", "clear"],
        ).transpose()

        columns = ["hour", "condition", "temp"]
        weather_condition = (
            "condition == 'clear' | condition == 'partly-cloudy' | "
            + "condition == 'cloudy'"
        )
        for day in forecasts["forecasts"]:
            hours_day = df.from_records(
                day["hours"],
                columns=columns,
            )
            hours = hours_day.loc[
                (hours_day["hour"] >= self.time_from)
                & (hours_day["hour"] < self.time_till)
            ]
            if not hours.empty:
                avg_day_temp = hours["temp"].mean().round(2)
                clearly_hours = hours.query(weather_condition).agg(
                    ["count"]
                )["condition"]["count"]
            else:
                avg_day_temp = None
                clearly_hours = None
            daily_averages.loc["day_temp", day["date"]] = avg_day_temp
            daily_averages.loc["clear", day["date"]] = clearly_hours
        return daily_averages

    def total_stat(self, data):
        """Вычисление итоговых средних."""
        return df(
            data.mean(axis="columns", numeric_only=True).round(2).transpose(),
            columns=["total_average"],
        )


class DataAggregationTask:
    """Объединение вычисленных данных."""

    def __init__(self, filename: str, queue):
        self.queue = queue
        self.filename: str = self.remove_file_if_exist(filename)

    def remove_file_if_exist(self, filename: str) -> str:
        """Удаляет файл, если такой уже был."""
        if os.path.exists(filename):
            os.remove(filename)
            logger.debug("Old file removed %s", filename)
        return filename

    def aggregate(self):
        """Соединение данных."""
        queue_item = self.queue.get()
        while queue_item:
            logger.debug(
                "DataCalculationResult for city %s get from queue",
                queue_item.city.at[0, 'city'],
            )
            self.save_to_file(data=queue_item)
            queue_item = self.queue.get()

    def check_empty_file(self) -> bool:
        """Проверка пустой файл или нет."""
        return os.path.getsize(self.filename) == 0

    def save_to_file(self, data):
        """Сохранение данных в файл."""
        with open(self.filename, "a+", encoding="utf-8") as file:
            city = data.city.rename(columns={"city": "Город/день"})
            daily = data.daily_averages.rename(
                index={"day_temp": "Температура, среднее",
                       "clear": "Без осадков, часов"}
            ).reset_index().rename(columns={"index": ""})
            averages = data.averages.rename(
                columns={"total_average": "Среднее"}).set_axis([0, 1])

            data_to_save = concat([city, daily, averages], axis=1)
            header_enabled = self.check_empty_file()
            data_to_save.to_csv(
                file,
                na_rep="",
                index=False,
                header=header_enabled,
            )


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
        ratings_df = self.get_ratings_df(ratings)

        aggregation = pd.read_csv(self.filename).rename(
            columns=lambda i: "" if i.startswith("Unnamed") else i)
        full = pd.concat([aggregation, ratings_df], axis=1)
        full.to_csv(self.filename, na_rep="", index=False, encoding="utf-8")

        top = min(ratings)
        best_city = [idx for idx, rating in enumerate(ratings) if rating == top]
        self.comfortables = operator.itemgetter(*best_city)(cities)

    def get_cities_ratings(self) -> Tuple[List[Any], List[int]]:
        """Получение рейтинга городов."""
        df = read_csv(self.filename, usecols=["Город/день", "Среднее"])
        final_df = concat(
            [
                df[::2]["Среднее"].reset_index(drop=True),
                df[1::2]["Среднее"].reset_index(drop=True),
            ],
            axis=1,
        )
        cities_list = list(df[::2]["Город/день"])
        data_to_rank = list(final_df.itertuples(index=False, name=None))

        data_sorted = sorted(
            list(set(data_to_rank)),
            key=lambda item: (item[0], item[1]),
            reverse=True
        )
        ratings = [data_sorted.index(idx) + 1 for idx in data_to_rank]
        return cities_list, ratings

    def get_ratings_df(self, ratings_list: list) -> df:
        """Подготовка датафрейма для объединения с основным."""
        ratings_tmp = df(ratings_list, columns=["Рейтинг"])
        ratings_len = len(ratings_tmp) * 2
        ratings_tmp.index = pd.RangeIndex(0, ratings_len, 2)
        ratings_zeros = pd.DataFrame(
            0,
            index=pd.RangeIndex(1, ratings_len, 2),
            columns=["Рейтинг"],
        )
        ratings = pd.concat([ratings_tmp, ratings_zeros]).sort_index()
        ratings.replace(0, "", inplace=True)
        return ratings
