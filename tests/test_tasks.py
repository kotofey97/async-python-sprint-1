import os
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue

from csv_diff import compare, load_csv

from models import CityModel
from tasks import (DataAggregationTask, DataAnalyzingTask, DataCalculationTask,
                   DataFetchingTask)
from tests.conftest import CITIES, PATH


def test_fetch(api_client, data_from_test_file):
    """Тестирование DataFetchingTask."""
    expect_data = data_from_test_file
    get_data = DataFetchingTask(api_client)
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(get_data.fetch, CITIES))
    assert len(results) == 4
    for item in results:
        assert isinstance(item, CityModel)
    city_list = [item.city for item in results]
    assert city_list == CITIES
    assert results == expect_data


def test_calc(api_client):
    """Тестирование DataCalculationTask."""
    city = "MOSCOW"

    fetched = DataFetchingTask(api_client).fetch(city)
    calculator = DataCalculationTask(Queue(), 9, 19)
    calculated = calculator.calculate(fetched)

    expect_keys = ['city', 'clear_avg', 'dates', 'temp_avg']
    assert calculated['city'] == city
    assert calculated['temp_avg'] == 13.82
    assert calculated['clear_avg'] == 1.25
    assert sorted(calculated.keys()) == sorted(expect_keys)


def test_aggregation(api_client):
    """Тестирование DataAggregationTask."""
    testing_file = PATH + "test_expected.csv"
    file_to_test = PATH + "test.csv"
    try:
        os.remove(file_to_test)
    except OSError:
        pass

    calculation_task = DataCalculationTask(Queue(), 9, 19)
    aggregator = DataAggregationTask(file_to_test, Queue())
    for city in CITIES:
        fetched = DataFetchingTask(api_client).fetch(city)
        calculates = calculation_task.calculate(data=fetched)
        aggregator.aggregate(data=calculates)
    diff = compare(
        load_csv(open(testing_file, "r", encoding='utf-8')),
        load_csv(open(file_to_test, "r", encoding='utf-8')),
    )
    assert diff == {
        "added": [],
        "removed": [],
        "changed": [],
        "columns_added": [],
        "columns_removed": [],
    }


def test_analyze():
    """
    Тестирование DataAnalyzingTask

    Кейс 1: проверка рейтинга
    Кейс 2: определение комфортного города если он 1
    Кейс 3: определение несколький комфорных городов

    """
    testing_file = PATH + "test_expected.csv"
    analyze = DataAnalyzingTask(filename=testing_file)
    cities, ratings = analyze.get_cities_ratings()
    assert cities == ['MOSCOW', 'CAIRO', 'NOVOSIBIRSK', 'BUCHAREST']
    assert ratings == [4, 1, 3, 2]

    analyze.set_comfortables(cities, ratings)
    assert analyze.comfortables == 'CAIRO'

    analyze.set_comfortables(['MOSCOW', 'CAIRO', 'NOVOSIBIRSK'], [1, 1, 2])
    assert analyze.comfortables == ('MOSCOW', 'CAIRO')
