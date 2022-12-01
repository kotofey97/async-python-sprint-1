import json
import os
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue

import pandas as pd
import pytest
from csv_diff import compare, load_csv
from pydantic import parse_obj_as

from models import CityModel, DataCalculationResult, ForecastsModel
from tasks import DataAggregationTask, DataCalculationTask, DataFetchingTask

CITIES = ["MOSCOW", "CAIRO", "NOVOSIBIRSK", "BUCHAREST"]
PATH = "tests/fixtures/"


def read_json_file(path, file):
    """Загрузка данных из json"""
    with open(f"{path}{file}", "r", encoding="utf-8") as file:
        try:
            data = json.load(file)
        except Exception:
            data = None
    return data


@pytest.fixture()
def fixture_city():
    """Фикстура DataCalculationResult"""
    city = pd.read_csv(f"{PATH}city_moscow.csv")
    daily_averages = pd.read_csv(
        f"{PATH}daily_averages_moscow.csv", index_col=0)
    averages = pd.read_csv(
        f"{PATH}averages_moscow.csv", index_col=0
    )
    city_data = DataCalculationResult(
        city=city,
        daily_averages=daily_averages,
        averages=averages
    )
    return city_data


@pytest.fixture()
def data_from_test_file():
    """Данные для тестируемых городов."""
    data = read_json_file(PATH, 'test.json')
    result = list()

    for city, item in zip(CITIES, data):
        data = parse_obj_as(ForecastsModel, item)
        result.append(CityModel(city=city, forecasts=data))
    return result


def test_fetch(data_from_test_file):
    data = data_from_test_file
    get_data = DataFetchingTask()
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(get_data.fetch, CITIES))
    data == results


def test_calc(fixture_city):
    city = "MOSCOW"

    calculator = DataCalculationTask(Queue(), 9, 19)
    fetched = DataFetchingTask().fetch(city)
    calculated = calculator.calculate(fetched)

    assert pd.testing.assert_frame_equal(
        calculated.city, fixture_city.city) is None
    assert pd.testing.assert_frame_equal(
        calculated.daily_averages, fixture_city.daily_averages) is None
    assert pd.testing.assert_frame_equal(
        calculated.averages, fixture_city.averages) is None


def test_aggregation():
    testing_file = PATH + "test_expected.csv"
    file_to_test = PATH + "test.csv"
    try:
        os.remove(file_to_test)
    except OSError:
        pass

    calculation_task = DataCalculationTask(Queue(), 9, 19)
    aggregator = DataAggregationTask(file_to_test, Queue())
    for city in CITIES:
        fetched = DataFetchingTask().fetch(city)
        calculates = calculation_task.calculate(data=fetched)
        aggregator.aggregate(data=calculates)
    diff = compare(
        load_csv(open(testing_file, "r")),
        load_csv(open(file_to_test, "r")),
    )
    assert diff == {
        "added": [],
        "removed": [],
        "changed": [],
        "columns_added": [],
        "columns_removed": [],
    }
