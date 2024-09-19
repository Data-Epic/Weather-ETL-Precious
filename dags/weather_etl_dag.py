from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import TaskInstance
import os
from dotenv import load_dotenv
import pandas as pd
from typing import cast

from dags.src.utils import (
    fetch_multiple_weather_data,
    process_weather_response,
    add_cities,
    add_current_weather,
    add_full_records,
)
from dags.src.database import get_db_conn

import logging

logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.getenv("API_KEY")
DB_URL = os.getenv("DB_URL")

if not API_KEY or not DB_URL:
    logger.error(
        "API Key or Database URL not found. Make sure they're defined in the .env file"
    )
    raise ValueError("API Key or Database URL not found")

api_key = cast(str, API_KEY)
db_url = cast(str, DB_URL)

cities = ["London", "Lagos", "Paris", "Dubai"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 18),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="weather_etl_dag_hourly",
    default_args=default_args,
    description="Weather ETL DAG to fetch and load weather data, runs hourly",
    schedule=timedelta(hours=1),
)


def extract_weather_data() -> dict:
    """Task to extract weather data from API"""
    try:
        response = fetch_multiple_weather_data(cities, api_key)
        if not response:
            logger.error("Failed to fetch weather data from the API.")
            raise ValueError("API Response failed.")
        logger.info("Data fetched from API")
        return response
    except Exception as e:
        logger.error(f"Error in extract_weather_data: {e}, Response: {response}")
        raise


def transform_weather_data(ti: TaskInstance) -> None:
    """Transform weather_response dict"""
    try:
        response: dict = ti.xcom_pull(task_ids="extract_weather_data")
        if not response:
            logger.error("Data not found.")
            raise ValueError("No data to process.")

        processed_data: dict = process_weather_response(response)
        ti.xcom_push(key="processed_data", value=processed_data)
        logger.info("Weather data processed")
    except Exception as e:
        response = ti.xcom_pull(task_ids="extract_weather_data")
        logger.error(f"Error in transform_weather_data: {e}, Response: {response}")
        raise


def load_weather_data(ti: TaskInstance) -> None:
    """Load weather dataframes into database"""
    try:
        processed_data: dict = ti.xcom_pull(
            task_ids="transform_weather_data", key="processed_data"
        )
        if not processed_data:
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
        logger.info("New data added to database")
    except Exception as e:
        response = ti.xcom_pull(task_ids="transform_weather_data", key="processed_data")
        logger.error(f"Error in load_weather_data: {e}, Response: {response}")
        raise


# Airflow tasks using PythonOperator
extract_task = PythonOperator(
    task_id="extract_weather_data",
    python_callable=extract_weather_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_weather_data",
    python_callable=transform_weather_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_weather_data",
    python_callable=load_weather_data,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task

if __name__ == "__main__":
    dag.test()
