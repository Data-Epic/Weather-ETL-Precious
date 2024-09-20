import pandas as pd

from .utils import (
    get_env,
    fetch_multiple_weather_data,
    process_weather_response,
    add_cities,
    add_current_weather,
    add_full_records,
)
from .database import get_db_conn
from .logger_config import info_logger, error_logger

api_key, db_url = get_env()


def extract_weather_data(cities: list[str], api_key: str = api_key) -> dict:
    """Task to extract weather data from API"""
    try:
        response = fetch_multiple_weather_data(cities, api_key)
        if not response:
            error_logger.error("Failed to fetch weather data from the API.")
            raise ValueError("API Response failed.")
        info_logger.info("Data fetched from API")
        return response
    except Exception as e:
        error_logger.error(f"Error occured when extracting data: {e}", exc_info=True)
        raise


def transform_weather_data(weather_response: dict) -> dict:
    """Transform weather_response dict"""
    try:
        if not weather_response or weather_response == {}:
            error_logger.error("No data to process.")
            raise ValueError("No data to process.")
        processed_data = process_weather_response(weather_response)
        info_logger.info("Weather data processed")
        return processed_data
    except Exception as e:
        error_logger.error(f"Error occured during transformation: {e}", exc_info=True)
        raise


def load_weather_data(processed_data: dict, db_url: str = db_url) -> None:
    """Load weather dataframes into database"""
    try:
        if not processed_data or processed_data == {}:
            error_logger.error("No processed data to load.")
            raise ValueError("No processed data to load.")

        # Extract data frames from processed data
        city_df: pd.DataFrame = processed_data["cities"]
        full_record_df: pd.DataFrame = processed_data["full_records"]
        current_weather_df: pd.DataFrame = processed_data["current_weather"]

        conn = get_db_conn(db_url)

        # Insert data into the database
        city_cache: dict = add_cities(conn, city_df)
        add_full_records(conn, full_record_df, city_cache)
        add_current_weather(conn, current_weather_df, city_cache)
        info_logger.info("New data added to database")

        conn.close()
    except Exception as e:
        error_logger.error(f"Error in loading data to database: {e}", exc_info=True)
        raise
