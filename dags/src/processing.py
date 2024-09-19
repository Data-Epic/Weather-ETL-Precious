import pandas as pd
from datetime import datetime
import math

from dags.src.logger_config import error_logger


def load_to_df(weather_data: dict) -> pd.DataFrame:
    """
    Loads multiple JSON responses into a pandas DataFrame.
    """
    records = []
    try:
        for city, data in weather_data.items():
            record = {
                "name": city,
                "latitude": data["coord"]["lat"],
                "longitude": data["coord"]["lon"],
                "description": data["weather"][0]["description"],
                "temp_k": data["main"]["temp"],
                "feels_like_k": data["main"]["feels_like"],
                "temp_min_k": data["main"]["temp_min"],
                "temp_max_k": data["main"]["temp_max"],
                "pressure": data["main"]["pressure"],
                "humidity": data["main"]["humidity"],
                "sea_level": data["main"]["sea_level"],
                "grnd_level": data["main"]["grnd_level"],
                "visibility": data["visibility"],
                "wind_speed": data["wind"]["speed"],
                "wind_deg": data["wind"]["deg"],
                "rain_volume": data.get("rain", {}).get("1h", 0),
                "cloud_cover": data["clouds"]["all"],
                "sunrise": datetime.fromtimestamp(data["sys"]["sunrise"]).time(),
                "sunset": datetime.fromtimestamp(data["sys"]["sunset"]).time(),
                "country": data["sys"]["country"],
                "timezone_offset": data["timezone"],
            }
            records.append(record)
    except KeyError as e:
        error_logger.error(f"Error processing data for {city}: {e}", exc_info=True)
    except Exception as e:
        error_logger.error(f"An error occurred: {e}", exc_info=True)

    df = pd.DataFrame(records)
    return df


def kelvin_to_celsius(kelvin: float) -> float:
    """
    Convert temperature in kelvin to celsius.
    Formula: kelvin -273.15.
    """
    try:
        celsius = kelvin - 273.15
        return round(celsius, 2)
    except TypeError as e:
        error_logger.error(
            f"TypeError: Invalid type for kelvin value: {kelvin}. Error: {e}",
            exc_info=True,
        )
        return float(math.nan)


def offset_to_utc_str(offset: int) -> str:
    """
    Convert a timezone offset in seconds to a UTC timezone string
    in the format "UTC±HH:MM".
    """
    try:
        offset_hours = abs(offset // 3600)
        offset_minutes = (abs(offset) % 3600) // 60

        sign = "+" if offset >= 0 else "-"

        return f"UTC{sign}{offset_hours:02}:{offset_minutes:02}"
    except Exception as e:
        error_logger.error(
            f"An error occurred while converting offset to UTC string: {e}",
            exc_info=True,
        )
        return "UTC+00:00"


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform dataframe columns to appropriate data types.
    Also adds in new columns:
    - temp_celsius: temperature in celsius
    - temp_utc: timezone in 'UTC+xx:xx' format
    """
    try:
        # Convert to string
        df["name"] = df["name"].astype("string")
        df["description"] = df["description"].astype("string")
        df["country"] = df["country"].astype("string")

        df["temp_c"] = df["temp_k"].apply(kelvin_to_celsius)
        df["timezone_utc"] = (
            df["timezone_offset"].apply(offset_to_utc_str).astype("string")
        )

        return df
    except KeyError as e:
        error_logger.error(f"KeyError: Missing key {e} in the DataFrame", exc_info=True)
        return pd.DataFrame()
    except Exception as e:
        error_logger.error(
            f"An error occurred while transforming data: {e}", exc_info=True
        )
        return pd.DataFrame()


def create_city_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a DataFrame for city data from the main DataFrame.
    """
    try:
        city_df = df[
            [
                "name",
                "latitude",
                "longitude",
                "country",
                "timezone_offset",
                "timezone_utc",
            ]
        ].copy()
        return city_df
    except KeyError as e:
        error_logger.error(f"KeyError: Missing key {e} in the DataFrame", exc_info=True)
        return pd.DataFrame()
    except Exception as e:
        error_logger.error(
            f"An error occurred while creating city DataFrame: {e}", exc_info=True
        )
        return pd.DataFrame()


def create_full_record_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a DataFrame for weather records from the main DataFrame.
    """
    try:
        full_record_df = df[
            [
                "name",
                "description",
                "temp_k",
                "temp_c",
                "feels_like_k",
                "temp_min_k",
                "temp_max_k",
                "pressure",
                "humidity",
                "sea_level",
                "grnd_level",
                "visibility",
                "wind_speed",
                "wind_deg",
                "rain_volume",
                "cloud_cover",
                "sunrise",
                "sunset",
                "timezone_utc",
            ]
        ].copy()
        return full_record_df
    except KeyError as e:
        error_logger.error(f"KeyError: Missing key {e} in the DataFrame", exc_info=True)
        return pd.DataFrame()
    except Exception as e:
        error_logger.error(
            f"An error occurred while creating full record DataFrame: {e}",
            exc_info=True,
        )
        return pd.DataFrame()


def create_current_weather_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a DataFrame for current weather data from the main DataFrame.
    """
    try:
        current_weather_df = df[
            [
                "name",
                "description",
                "temp_k",
                "temp_c",
                "pressure",
                "humidity",
                "wind_speed",
                "rain_volume",
                "timezone_utc",
            ]
        ].copy()
        return current_weather_df
    except KeyError as e:
        error_logger.error(f"KeyError: Missing key {e} in the DataFrame", exc_info=True)
        return pd.DataFrame()
    except Exception as e:
        error_logger.error(
            f"An error occurred while creating current weather DataFrame: {e}",
            exc_info=True,
        )
        return pd.DataFrame()
