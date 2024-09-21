import requests
import time
from ratelimit import limits, sleep_and_retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import pandas as pd
from sqlalchemy.orm import Session
import os
from dotenv import load_dotenv

from typing import Optional

from .logger_config import info_logger, error_logger
from .processing import (
    load_to_df,
    transform_data,
    create_city_df,
    create_full_record_df,
    create_current_weather_df,
)
from .models import City, CurrentWeather, FullRecord

load_dotenv()


def get_env() -> tuple[str, str]:
    """Get weather api key and database url environment variables"""
    api_key = os.getenv("API_KEY")
    db_url = os.getenv("DB_URL")

    if not api_key or not db_url:
        error_logger.error(
            "API_KEY or DB_URL not found. Make sure they're defined in the .env file"
        )
        raise ValueError("API Key or Database URL not found")
    else:
        return api_key, db_url


@sleep_and_retry
@limits(calls=60, period=60)  # limits API calls to 60 per minute
def fetch_weather_data(city: str, api_key: str) -> dict:
    """Fetches weather data for a city."""
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    data = {}

    for _ in range(2):  # Retry up to 2 times for failed requests
        try:
            response = requests.get(base_url, params={"q": city, "appid": api_key})
            if response.status_code == 200:
                data = response.json()
                info_logger.info(f"Raw weather data for {city}: {json.dumps(data)}")
                return data
            else:
                error_logger.error(
                    f"Failed to fetch data for {city}: {response.status_code}",
                    exc_info=True,
                )
        except requests.exceptions.RequestException as e:
            error_logger.error(f"Error fetching data for {city}: {e}", exc_info=True)
        time.sleep(2)  # Wait 2 seconds before retrying
    return data


def fetch_multiple_weather_data(cities: list[str], api_key: str) -> dict:
    """Fetches weather data for multiple cities with rate limiting and concurrency."""
    weather_data = {}

    with ThreadPoolExecutor(max_workers=10) as executor:  # Up to 10 requests at once
        future_to_city = {
            executor.submit(fetch_weather_data, city, api_key): city for city in cities
        }

        for future in as_completed(future_to_city):
            city = future_to_city[future]
            try:
                data = future.result()
                if data:
                    weather_data[city] = data
            except Exception as e:
                error_logger.error(
                    f"Error fetching weather data for {city}: {e}", exc_info=True
                )
    return weather_data


def process_weather_response(weather_data: dict) -> dict:
    """
    Process the weather data response for multiple cities and return
    various DataFrames for different aspects of the data.
    """
    try:
        weather_df = load_to_df(weather_data)

        transformed_df = transform_data(weather_df)

        city_df = create_city_df(transformed_df)
        full_record_df = create_full_record_df(transformed_df)
        current_weather_df = create_current_weather_df(transformed_df)

        info_logger.info(f"city_df: {city_df}")
        info_logger.info(f"full_record_df: {full_record_df}")
        info_logger.info(f"current_weather_df: {current_weather_df}")

        return {
            "cities": city_df,
            "full_records": full_record_df,
            "current_weather": current_weather_df,
        }
    except Exception as e:
        error_logger.error(
            f"An error occurred while processing the weather response: {e}",
            exc_info=True,
        )
        return {}


def add_city(session: Session, data: pd.Series) -> None:
    """Add a data for one city to db"""
    try:
        city = City(**data.to_dict())
        session.add(city)
        session.commit()
        info_logger.info(f"New record added to cities: {data}")
    except Exception as e:
        error_logger.error(f"Error adding record {data} to db: {e}", exc_info=True)
        session.rollback()


def add_cities(session: Session, city_df: pd.DataFrame) -> dict:
    """Add cities to db from city_df and return a dict of cities for caching"""
    city_cache = {}
    try:
        for _, row in city_df.iterrows():
            city_name = row["name"]
            city = session.query(City).filter_by(name=city_name).first()
            if not city:
                info_logger.info(
                    f"{city_name} doesn't exist in cities table. Adding it now..."
                )
                add_city(session, row)
                city = session.query(City).filter_by(name=city_name).first()
            city_cache[city_name] = city.id
    except Exception as e:
        error_logger.error(f"Error adding  cities to db: {e}", exc_info=True)
    return city_cache


def get_city_id(city_name: str, city_cache: dict) -> Optional[int]:
    """Get the city id from the database"""
    try:
        city_id = city_cache[city_name]
        if not city_id:
            info_logger.info(f"{city_name} not found in cache. Add to db")
            return None
        return city_id
    except Exception as e:
        error_logger.error(f"Error getting city id for {city_name}: {e}", exc_info=True)
        return None


def add_full_records(
    session: Session, full_record_df: pd.DataFrame, city_cache: dict
) -> None:
    """Add full weather records to db"""
    try:
        full_record_df["city_id"] = full_record_df["city_name"].map(city_cache)
        full_record_records = full_record_df.to_dict(orient="records")
        session.bulk_insert_mappings(FullRecord, full_record_records)
        session.commit()
        info_logger.info("Full weather records added to db")
    except Exception as e:
        error_logger.error(
            f"Error adding full weather records to db: {e}", exc_info=True
        )
        session.rollback()


def add_current_weather(
    session: Session, current_weather_df: pd.DataFrame, city_cache: dict
) -> None:
    """Add or update current weather data in the db"""
    try:
        current_weather_df["city_id"] = current_weather_df["city_name"].map(city_cache)
        current_weather_records = current_weather_df.to_dict(orient="records")
        for record in current_weather_records:
            city_id = record["city_id"]
            existing_record = (
                session.query(CurrentWeather).filter_by(city_id=city_id).first()
            )
            if existing_record:
                # Update the existing record
                for key, value in record.items():
                    setattr(existing_record, key, value)
                info_logger.info(f"Updated weather record for {city_id}")
            else:
                # Insert a new record
                new_record = CurrentWeather(**record)
                session.add(new_record)
                info_logger.info(f"Inserted new weather record for {city_id}")
        session.commit()
        info_logger.info("Current weather data updated")
    except Exception as e:
        error_logger.error(f"Error updating current weather table: {e}", exc_info=True)
        session.rollback()
