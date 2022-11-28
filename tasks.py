from api_client import YandexWeatherAPI
from data_objs import CityModel
from pandas import DataFrame as df
import operator

class DataFetchingTask:
    """Получение данных через API."""
    @staticmethod
    def fetch(city: str):
        result = YandexWeatherAPI().get_forecasting(city)
        return CityModel(city=city, forecasts=result)


class DataCalculationTask:

    def __init__(self, queue, hours_to_start, hours_to_finish) -> None:
        self.queue = queue
        self.hours_to_start = hours_to_start
        self.hours_to_finish = hours_to_finish
    
    def calculate(self, data):
        forecasts = data.forecasts.dict()
        city_name = data.city
        # print('calculate', forecasts)
        # print('city_name', city_name)
        self.daily_stat(forecasts)

    def daily_stat(self, forecasts):
        daily_averages = df(
            columns=["day_temp", "clear"],
        ).transpose()
        print('GGG', daily_averages)
        columns = ["hour", "condition", "temp"]
        types = {"hour": "int32", "temp": "int32"}
        

        for day in forecasts["forecasts"]:
            hours_day = df.from_records(
                day["hours"],
                columns=columns,
            ).astype(types)
            hours = hours_day.loc[(hours_day["hour"] >= self.hours_to_start) & (hours_day["hour"] < self.hours_to_finish)]
            if not hours.empty:
                avg_day_temp = hours["temp"].mean().round(2)
                clearly_hours = hours.query("condition == 'clear' | condition == 'partly-cloudy' | condition == 'cloudy' | condition == 'overcast'").agg(
                    ["count"]
                )["condition"]["count"]
            else:
                avg_day_temp = None
                clearly_hours = None
            daily_averages.loc["day_temp", day["date"]] = avg_day_temp
            daily_averages.loc["clear", day["date"]] = clearly_hours

        print('HHHHHH', daily_averages)
        return daily_averages





class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass
