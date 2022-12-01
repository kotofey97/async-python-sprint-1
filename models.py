from pandas import DataFrame
from pydantic import BaseModel


class InfoByHourModel(BaseModel):
    hour: int
    temp: int
    condition: str


class DateModel(BaseModel):
    date: str
    hours: list[InfoByHourModel]


class ForecastsModel(BaseModel):
    forecasts: list[DateModel]


class CityModel(BaseModel):
    city: str
    forecasts: ForecastsModel


class DataCalculationResult(BaseModel):
    city: DataFrame
    daily_averages: DataFrame
    averages: DataFrame

    class Config:
        arbitrary_types_allowed = True
