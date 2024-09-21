import pytest
import pandas as pd
import os
from dags.src.utils import (
    fetch_weather_data,
    fetch_multiple_weather_data,
    process_weather_response,
    add_cities,
    get_city_id,
    add_current_weather,
    add_full_records,
    get_env,
)

from dags.src.database import get_db_conn
from dags.src.models import City, CurrentWeather, FullRecord

API_KEY = os.getenv("API_KEY")


@pytest.fixture(scope="module")
def test_db():
    db = get_db_conn("sqlite:///:memory:")
    yield db


def test_fetch_weather_data_success():
    city = "London"
    result = fetch_weather_data(city, API_KEY)

    assert result is not None
    assert "weather" in result
    assert "main" in result
    assert "wind" in result


def test_fetch_weather_data_failure():
    city = "InvalidCity"
    result = fetch_weather_data(city, API_KEY)

    assert result == {}


def test_fetch_multiple_weather_data():
    cities = ["London", "Paris"]
    result = fetch_multiple_weather_data(cities, API_KEY)

    assert len(result) == 2
    assert "London" in result
    assert "Paris" in result
    assert "weather" in result["London"]
    assert "weather" in result["Paris"]


@pytest.fixture(scope="module")
def weather_response():
    cities = ["London", "Lagos"]
    result = fetch_multiple_weather_data(cities, API_KEY)
    return result


def test_process_weather_response(weather_response):
    weather_data = weather_response
    result = process_weather_response(weather_data)

    assert "cities" in result
    assert "full_records" in result
    assert "current_weather" in result

    assert isinstance(result["cities"], pd.DataFrame)
    assert isinstance(result["full_records"], pd.DataFrame)
    assert isinstance(result["current_weather"], pd.DataFrame)

    assert not result["cities"].empty
    assert not result["full_records"].empty
    assert not result["current_weather"].empty


def test_add_cities(weather_response, test_db):
    data = process_weather_response(weather_response)
    city_df = data["cities"]
    result = add_cities(test_db, city_df)

    assert isinstance(result, dict)
    assert "London" in result
    assert "Lagos" in result

    for _, city_id in result.items():
        record = test_db.query(City).filter_by(id=city_id).first()
        assert record is not None
        assert record.id == city_id


def test_get_city_id():
    city_cache = {"London": "UK-London", "Lagos": "NG-Lagos"}

    city_name = "London"
    result = get_city_id(city_name, city_cache)
    assert result == "UK-London"

    city_name = "New York"
    result = get_city_id(city_name, city_cache)
    assert result is None


def test_add_full_records(test_db, weather_response):
    data = process_weather_response(weather_response)
    full_record_df = data["full_records"]
    city_cache = add_cities(test_db, data["cities"])

    add_full_records(test_db, full_record_df, city_cache)

    for _, city_id in city_cache.items():
        record = test_db.query(FullRecord).filter_by(city_id=city_id).first()
        assert record is not None
        assert record.city_id == city_id


def test_add_current_weather(test_db, weather_response):
    data = process_weather_response(weather_response)
    current_weather_df = data["current_weather"]
    city_cache = add_cities(test_db, data["cities"])

    add_current_weather(test_db, current_weather_df, city_cache)

    for _, city_id in city_cache.items():
        record = test_db.query(CurrentWeather).filter_by(city_id=city_id).first()
        assert record is not None
        assert record.city_id == city_id


def test_get_env(monkeypatch):
    # Set the environment variables using monkeypatch
    monkeypatch.setenv("API_KEY", "test_api_key")
    monkeypatch.setenv("DB_URL", "sqlite:///:memory:")

    # Test for when API_KEY and DB_URL exist
    api_key, db_url = get_env()
    assert api_key == "test_api_key"
    assert db_url == "sqlite:///:memory:"

    # Test for when API_KEY and DB_URL do not exist
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.delenv("DB_URL", raising=False)
    with pytest.raises(ValueError, match="API Key or Database URL not found"):
        get_env()
