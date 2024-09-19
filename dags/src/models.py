from typing import Any, Type

from sqlalchemy import (
    TIME,
    TIMESTAMP,
    BigInteger,
    Column,
    Float,
    ForeignKey,
    Integer,
    Sequence,
    String,
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base: Type[Any] = declarative_base()


class City(Base):
    __tablename__ = "cities"

    id = Column(Integer, Sequence("id"), primary_key=True)
    name = Column(String(20), nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    country = Column(String(2), nullable=False)
    timezone_offset = Column(BigInteger, nullable=False)
    timezone_utc = Column(String(10), nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=False)
    updated_at = Column(
        TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    full_records = relationship("FullRecord", back_populates="city")
    current_weather = relationship("CurrentWeather", back_populates="city")

    def __repr__(self) -> str:
        return (
            f"<City(id={self.id}, name={self.name}, latitude={self.latitude}, longitude={self.longitude}, "
            f"country={self.country}, timezone_offset={self.timezone_offset}, timezone_utc={self.timezone_utc}, "
            f"created_at={self.created_at}, updated_at={self.updated_at})>"
        )


class FullRecord(Base):
    __tablename__ = "full_records"

    id = Column(Integer, Sequence("id"), primary_key=True)
    city_id = Column(Integer, ForeignKey("cities.id"), nullable=False)
    city = relationship("City", back_populates="full_records")
    description = Column(String(100), nullable=False)
    temp_k = Column(Float, nullable=False)
    temp_c = Column(Float, nullable=False)
    feels_like_k = Column(Float, nullable=False)
    temp_min_k = Column(Float, nullable=False)
    temp_max_k = Column(Float, nullable=False)
    pressure = Column(Integer, nullable=False)
    humidity = Column(Integer, nullable=False)
    sea_level = Column(Integer, nullable=False)
    grnd_level = Column(Integer, nullable=False)
    visibility = Column(Integer, nullable=False)
    wind_speed = Column(Float, nullable=False)
    wind_deg = Column(Integer, nullable=False)
    rain_volume = Column(Float, nullable=False)
    cloud_cover = Column(Integer, nullable=False)
    sunrise = Column(TIME, nullable=False)
    sunset = Column(TIME, nullable=False)
    recorded_at = Column(TIMESTAMP, server_default=func.now(), nullable=False)
    timezone_utc = Column(String(10), nullable=False)

    def __repr__(self) -> str:
        return (
            f"<FullRecord(id={self.id}, city_id={self.city_id}, city_name={self.city.name}, "
            f" temp_k={self.temp_k},temp_c={self.temp_c}, feels_like_k={self.feels_like_k}, "
            f"temp_min_k={self.temp_min_k}, temp_max_k={self.temp_max_k}, "
            f"pressure={self.pressure}, humidity={self.humidity}, sea_level={self.sea_level}, grnd_level={self.grnd_level}, "
            f"visibility={self.visibility}, wind_speed={self.wind_speed}, wind_deg={self.wind_deg}, "
            f"rain_volume={self.rain_volume}, cloud_cover={self.cloud_cover}, sunrise={self.sunrise}, sunset={self.sunset}, "
            f"recorded_at={self.recorded_at}, timezone_utc={self.timezone_utc})>"
        )


class CurrentWeather(Base):
    __tablename__ = "current_weather"

    id = Column(Integer, Sequence("id"), primary_key=True)
    city_id = Column(Integer, ForeignKey("cities.id"), nullable=False)
    city = relationship("City", back_populates="current_weather")
    description = Column(String(100), nullable=False)
    temp_k = Column(Float, nullable=False)
    temp_c = Column(Float, nullable=False)
    pressure = Column(Integer, nullable=False)
    humidity = Column(Integer, nullable=False)
    wind_speed = Column(Float, nullable=False)
    rain_volume = Column(Float, nullable=False)
    recorded_at = Column(TIMESTAMP, server_default=func.now(), nullable=False)
    timezone_utc = Column(String(10), nullable=False)

    def __repr__(self) -> str:
        return (
            f"<CurrentWeather(id={self.id}, city_id={self.city_id}, city_name={self.city.name}, "
            f"temp_k={self.temp_k}, temp_c={self.temp_c}, pressure={self.pressure}, humidity={self.humidity}, "
            f"wind_speed={self.wind_speed}, weather_description={self.description}, "
            f"recorded_at={self.recorded_at}, timezone_utc={self.timezone_utc})>"
        )
