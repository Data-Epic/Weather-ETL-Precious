from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import TaskInstance, Variable
import sys
import json

from dags.src.pipeline import (
    extract_weather_data,
    transform_weather_data,
    load_weather_data,
)

import logging

logger = logging.getLogger(__name__)

sys.path.append("/dags")


def get_cities(ti: TaskInstance) -> None:
    """Fetch cities from Airflow Variables or log an error if not set"""
    try:
        cities_str = Variable.get("CITIES")
        cities = [city.strip() for city in cities_str.split(",")]
        logger.info(f"Cities: {cities}")
        ti.xcom_push(key="cities", value=cities)
    except KeyError:
        logger.error(
            "Airflow Variable 'CITIES' is not set. Please set it in the Airflow UI under Admin > Variables. "
            "Example - key: CITIES, value: London, Ife, Lagos"
        )
        raise
    except json.JSONDecodeError:
        logger.error(
            "Airflow Variable 'cities' is not a valid JSON. Please set it to a valid JSON array. "
            "Example - key: CITIES, value: London, Ife, Lagos"
        )
        raise


def extract_data(ti: TaskInstance) -> None:
    """Task to extract weather data from API"""
    try:
        cities = ti.xcom_pull(task_ids="get_cities", key="cities")
        data = extract_weather_data(cities)
        ti.xcom_push(key="weather_data", value=data)
    except Exception as e:
        logger.error(f"Error in extract_data: {e}")
        raise


def transform_data(ti: TaskInstance) -> None:
    """Task to transform weather data"""
    try:
        raw_data = ti.xcom_pull(task_ids="extract_weather_data", key="weather_data")
        if not raw_data:
            logger.error("No data found for transformation.")
            raise ValueError("No data to transform.")
        transformed_data = transform_weather_data(raw_data)
        ti.xcom_push(key="transformed_data", value=transformed_data)
        logger.info("Data transformed successfully")
    except Exception as e:
        response = ti.xcom_pull(task_ids="extract_weather_data")
        logger.error(f"Error in transform_data: {e}, Response: {response}")
        raise


def load_data(ti: TaskInstance) -> None:
    """Task to load weather data into the destination"""
    try:
        transformed_data = ti.xcom_pull(
            task_ids="transform_weather_data", key="transformed_data"
        )
        if not transformed_data:
            logger.error("No transformed data found for loading.")
            raise ValueError("No data to load.")
        load_weather_data(transformed_data)
        logger.info("Data loaded successfully")
    except Exception as e:
        response = ti.xcom_pull(task_ids="transform_weather_data")
        logger.error(f"Error in load_data: {e}, Response: {response}")
        raise


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 19),
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
    catchup=False,
)

cities_task = PythonOperator(
    task_id="get_cities",
    python_callable=get_cities,
    dag=dag,
)

extract_task = PythonOperator(
    task_id="extract_weather_data",
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_weather_data",
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_weather_data",
    python_callable=load_data,
    dag=dag,
)


cities_task >> extract_task >> transform_task >> load_task

if __name__ == "__main__":
    dag.test()
