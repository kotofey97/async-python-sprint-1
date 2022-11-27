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
