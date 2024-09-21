import pytest
from dags.src.pipeline import (
    extract_weather_data,
    transform_weather_data,
    load_weather_data,
)
from dags.src.database import get_db_conn
import os


@pytest.fixture(scope="module")
def get_cities():
    return ["London", "Lagos"]


def test_extract_weather_data_success(get_cities):
    cities = get_cities
    response = extract_weather_data(cities)
    assert response is not None
    assert len(response) == 2
    assert "London" in response


def test_extract_weather_data_failure(get_cities):
    # Test with invalid api_key
    cities = get_cities
    with pytest.raises(ValueError):
        extract_weather_data(cities, "invalid_api_key")


def test_transform_weather_data_success(get_cities):
    data = extract_weather_data(get_cities)
    result = transform_weather_data(data)
    assert result is not None
    assert "cities" in result
    assert "full_records" in result
    assert "current_weather" in result


def test_transform_weather_data_failure():
    with pytest.raises(ValueError):
        transform_weather_data({})


@pytest.fixture(scope="module")
def get_processed_data(get_cities):
    data = extract_weather_data(get_cities)
    processed_data = transform_weather_data(data)
    return processed_data


def test_load_weather_data_success(get_processed_data):
    processed_data = get_processed_data
    test_db_url = "sqlite:///test.db"
    load_weather_data(processed_data, test_db_url)

    # Test that data was added to db
    session = get_db_conn(test_db_url)
    tables = session.execute(
        "SELECT name FROM sqlite_master WHERE type='table';"
    ).fetchall()
    assert len(tables) == 3, "No tables found in the database."

    count = session.execute("SELECT COUNT(*) FROM cities;").scalar()
    assert count > 0, "No data found in the cities table."

    session.close()

    # Delete the test database file
    os.remove("test.db")


def test_load_weather_data_failure():
    with pytest.raises(ValueError):
        load_weather_data({})


def test_load_weather_data_invalid_db(get_processed_data):
    processed_data = get_processed_data
    invalid_db_url = "invalid_db_url"
    with pytest.raises(Exception):
        load_weather_data(processed_data, invalid_db_url)
