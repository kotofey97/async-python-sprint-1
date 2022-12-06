import json

import pytest
from pydantic import parse_obj_as

from models import CityModel, ForecastsModel

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


class MockClient():
    def get_forecasting(self, city_name):
        res = read_json_file(PATH, 'test.json')
        for item in res:
            if item["geo_object"]["locality"]["name"].upper() == city_name:
                return item


@pytest.fixture()
def api_client():
    return MockClient()


@pytest.fixture()
def data_from_test_file():
    """Данные для тестируемых городов."""
    data = read_json_file(PATH, 'test.json')
    result = list()

    for city, item in zip(CITIES, data):
        data = parse_obj_as(ForecastsModel, item)
        result.append(CityModel(city=city, forecasts=data))
    return result
