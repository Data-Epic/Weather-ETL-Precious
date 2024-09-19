import pytest
import pandas as pd
from dags.src.processing import (
    load_to_df,
    kelvin_to_celsius,
    offset_to_utc_str,
    transform_data,
    create_city_df,
    create_full_record_df,
    create_current_weather_df,
)
import math


@pytest.fixture
def valid_data():
    return {
        "Nkpor": {
            "coord": {"lon": 6.8446, "lat": 6.1516},
            "weather": [
                {"id": 500, "main": "Rain", "description": "light rain", "icon": "10n"}
            ],
            "base": "stations",
            "main": {
                "temp": 295.75,
                "feels_like": 296.6,
                "temp_min": 295.75,
                "temp_max": 295.75,
                "pressure": 1014,
                "humidity": 97,
                "sea_level": 1014,
                "grnd_level": 995,
            },
            "visibility": 10000,
            "wind": {"speed": 2.3, "deg": 204, "gust": 6.91},
            "rain": {"1h": 0.19},
            "clouds": {"all": 100},
            "dt": 1726605936,
            "sys": {"country": "NG", "sunrise": 1726550572, "sunset": 1726594282},
            "timezone": 3600,
            "id": 2328811,
            "name": "Nkpor",
            "cod": 200,
        }
    }


def test_load_to_df_valid_data(valid_data):
    df = load_to_df(valid_data)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df["latitude"][0] == 6.1516
    assert df["longitude"][0] == 6.8446
    assert df["description"][0] == "light rain"
    assert df["temp_k"][0] == 295.75
    assert df["feels_like_k"][0] == 296.6
    assert df["temp_min_k"][0] == 295.75
    assert df["temp_max_k"][0] == 295.75
    assert df["pressure"][0] == 1014
    assert df["humidity"][0] == 97
    assert df["sea_level"][0] == 1014
    assert df["grnd_level"][0] == 995
    assert df["visibility"][0] == 10000
    assert df["wind_speed"][0] == 2.3
    assert df["wind_deg"][0] == 204
    assert df["rain_volume"][0] == 0.19
    assert df["cloud_cover"][0] == 100
    assert df["country"][0] == "NG"
    assert df["name"][0] == "Nkpor"
    assert df["timezone_offset"][0] == 3600


def test_load_to_df_missing_key(valid_data):
    invalid_data = valid_data.copy()
    del invalid_data["Nkpor"]["coord"]
    df = load_to_df(invalid_data)
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_load_to_df_invalid_data_type():
    invalid_data = "This is not a dictionary"
    df = load_to_df(invalid_data)
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_kelvin_to_celsius():
    assert kelvin_to_celsius(300) == 26.85
    assert kelvin_to_celsius(0) == -273.15
    assert kelvin_to_celsius(-1) == -274.15
    assert math.isnan(kelvin_to_celsius("invalid"))


def test_offset_to_utc_str():
    assert offset_to_utc_str(3600) == "UTC+01:00"
    assert offset_to_utc_str(-3600) == "UTC-01:00"
    assert offset_to_utc_str(0) == "UTC+00:00"
    assert offset_to_utc_str(5400) == "UTC+01:30"


def test_transform_data(valid_data):
    df = load_to_df(valid_data)
    transformed_df = transform_data(df)
    assert "temp_c" in transformed_df.columns
    assert "timezone_utc" in transformed_df.columns
    assert transformed_df["temp_c"][0] == kelvin_to_celsius(
        valid_data["Nkpor"]["main"]["temp"]
    )
    assert transformed_df["timezone_utc"][0] == offset_to_utc_str(
        valid_data["Nkpor"]["timezone"]
    )


def test_create_city_df(valid_data):
    df = load_to_df(valid_data)
    transformed_df = transform_data(df)
    city_df = create_city_df(transformed_df)
    assert "name" in city_df.columns
    assert "latitude" in city_df.columns
    assert "longitude" in city_df.columns
    assert "country" in city_df.columns
    assert "timezone_offset" in city_df.columns
    assert "timezone_utc" in city_df.columns


def test_create_full_record_df(valid_data):
    df = load_to_df(valid_data)
    transformed_df = transform_data(df)
    full_record_df = create_full_record_df(transformed_df)
    assert "description" in full_record_df.columns
    assert "temp_k" in full_record_df.columns
    assert "temp_c" in full_record_df.columns
    assert "feels_like_k" in full_record_df.columns
    assert "temp_min_k" in full_record_df.columns
    assert "temp_max_k" in full_record_df.columns
    assert "pressure" in full_record_df.columns
    assert "humidity" in full_record_df.columns
    assert "sea_level" in full_record_df.columns
    assert "grnd_level" in full_record_df.columns
    assert "visibility" in full_record_df.columns
    assert "wind_speed" in full_record_df.columns
    assert "wind_deg" in full_record_df.columns
    assert "rain_volume" in full_record_df.columns
    assert "cloud_cover" in full_record_df.columns
    assert "sunrise" in full_record_df.columns
    assert "sunset" in full_record_df.columns
    assert "timezone_utc" in full_record_df.columns


def test_create_current_weather_df(valid_data):
    df = load_to_df(valid_data)
    transformed_df = transform_data(df)
    current_weather_df = create_current_weather_df(transformed_df)
    assert "description" in current_weather_df.columns
    assert "temp_k" in current_weather_df.columns
    assert "temp_c" in current_weather_df.columns
    assert "pressure" in current_weather_df.columns
    assert "humidity" in current_weather_df.columns
    assert "wind_speed" in current_weather_df.columns
    assert "rain_volume" in current_weather_df.columns
    assert "timezone_utc" in current_weather_df.columns
