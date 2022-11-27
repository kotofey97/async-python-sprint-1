from api_client import YandexWeatherAPI
from data_objs import CityModel

class DataFetchingTask:
    """Получение данных через API."""
    @staticmethod
    def fetch(city: str):
        result = YandexWeatherAPI().get_forecasting(city)
        # print(result)
        return CityModel(city=city, forecasts=result)


class DataCalculationTask:
    pass


class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass
