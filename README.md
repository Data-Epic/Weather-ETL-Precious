# Weather ETL Pipeline Using Airflow and Docker

This project sets up an **ETL pipeline** using **Apache Airflow** running in a **Docker** environment. The pipeline retrieves weather data from the **OpenWeatherMap API** for multiple cities, processes it, and loads it into a **PostgreSQL** database.

The goal of this project is to automate the hourly fetching from API and loading to database of weather data using an Airflow DAG in a containerized  environment.

## Features

- **Dockerized Airflow and PostgreSQL**: The project uses Docker and Docker Compose to manage both Airflow and PostgreSQL services.
- **API Data Extraction**: Weather data is fetched using the OpenWeatherMap API.
- **PostgreSQL Data Storage**: The data is stored in a PostgreSQL database with a predefined schema.
- **Automated ETL Workflow**: Airflow manages the ETL process, scheduled to run hourly, ensuring that the weather data is updated regularly.
- **Database management with pgAdmin**: pgAdmin is used to manage the PostgreSQL database.
- **Monitoring and Logging**: Airflow's built-in features for monitoring and logging are utilized to track the pipeline workflow.
- **Testing and Dev Support**: The project includes a Makefile with commands to test the entire piple workflow and setup a development environment.

## Project Structure

```bash
/home/prechy/Dev/Weather-ETL-Precious/
├── dags/
│   ├── weather_dag.py      # Airflow DAG definition for the ETL process
│   └── src/                # Folder containing all Python scripts
├── tests/                  # Folder containing all tests
├── Dockerfile              # Dockerfile for configuring the Airflow environment
├── docker-compose.yml      # Docker-Compose file to run Airflow and PostgreSQL
├── requirements.txt        # Python dependencies for the project
├── Makefile                # Makefile for automating tasks
├── .pre-commit-config.yaml # Pre-commit configuration for code quality
├── README.md               # Project documentation
├── .github/workflows       # For Github Actions
└── images/
    └── weather_erd.png     # ERD for Weather data
```

---
## Setting up the project on your local

### Prerequisites

- **Docker**: Install [Docker]() and [Docker Compose]() on your machine.
- **OpenWeatherMap API**: Sign up for a free API key [here](https://www.google.com/url?q=https%3A%2F%2Fopenweathermap.org%2Fapi).


### Setup Instructions

#### 1. Clone the Repository

Start by cloning the project repository:

```bash
git clone https://github.com/Data-Epic/Weather-ETL-Precious.git
cd Weather-ETL-Precious
```
#### 2. Set up directories and environment variables
To avoid permission issues, you need to [set the airflow user](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#setting-the-right-airflow-user). Run this:

```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

This would create an  environment variable `AIRFLOW_UID` with the current user ID and save it to .env. By default, it is set to 1000 in the `docker_compose.yml`. You can change it by  setting the user value to the `$AIRFLOW_UID` environment variable.

You also need to set other  environment variables for airflow and your postgresdb (where you'd be storing your  data). You can do this by adding the following lines to your .env file:

```.env
#.env
AIRFLOW_UID=your_uid # get by running `id -u`

# connection to database for app
DB_USER=myadmin # set your own
DB_PASSWORD=mypassword # add your password
DB_NAME=weather_etl
DB_URL=postgresql+psycopg2://${DB_USER}:${DB_PASSWORD}@weather-db/${DB_NAME}

AIRFLOW__CORE__LOAD_EXAMPLES=False
```

#### 3. Configure OpenWeatherMap API

To fetch weather data, you need an API key from [OpenWeatherMap](https://openweathermap.org/api). Sign up for a free API key and update the `.env` file to include your API key:

```.env
API_KEY=your_weather_api_key
```

#### 4. Set up services

Make sure you have **Docker** and **Docker-Compose** installed. Then, build and start the Docker containers:

```bash
docker compose up --build
```

This will initialize the following services:
- **Airflow Webserver** accessible at [http://localhost:8080](http://localhost:8080). Log in with the default credentials (`airflow`/`airflow`).
- **PostgreSQL** accessible on port `5432`
- **pgAdmin** accessible on port [http://localhost:5050](http://localhost:5050)

#### Shutting down services and Cleaning up
To stop the services, run:

```bash
docker compose down
```

To stop and delete containers, delete volumes with database data and download images, run:

```bash
docker compose down --volumes --rmi all
```

This helps when there's a problem or you want to run a fresh set up.


## Fetching Data from the OpenWeatherMap API

The project uses the OpenWeatherMap API for fetching data. A free subscription allows up to 60 requests per minute. So there's need to handle requests for multiple cities, rate limit and retries. To do this, API calls for each would be made every second. Failed API calls would be retried 3 times 2 secs apart.

This is done with the `fetch_multiple_weather_data` function in `dags/src/processing.py`:
- Take a list of cities
- Fetch weather data for each city. Retry every 2 seconds, 3 times on failure.
- Add response to weather_data dictionary
- Fetch for the next city after 1 second
- Return  the weather_data dictionary

**Example API response for London**:
```
{
  "coord": {"lon": -0.1257, "lat": 51.5085},
  "weather": [
    {
      "id": 802,
      "main": "Clouds",
      "description": "scattered clouds",
      "icon": "03n"
    }
  ],
  "base": "stations",
  "main": {
    "temp": 289.9,
    "feels_like": 289.8,
    "temp_min": 288.58,
    "temp_max": 290.8,
    "pressure": 1027,
    "humidity": 83,
    "sea_level": 1027,
    "grnd_level": 1022
  },
  "visibility": 10000,
  "wind": {"speed": 3.09, "deg": 40},
  "clouds": {"all": 40},
  "dt": 1726695331,
  "sys": {
    "type": 2,
    "id": 2075535,
    "country": "GB",
    "sunrise": 1726638042,
    "sunset": 1726682912
  },
  "timezone": 3600,
  "id": 2643743,
  "name": "London",
  "cod": 200
}
```


## Data Modelling and Ingestion
The weather_data dictionary from fetching weather data is transformed and loaded in 3 different pandas dataframes:

```
- city_df: contains location-based data
- current_weather_df: contains basic weather data
- full_records_df: contains full weather data
```

The project uses a PostgreSQL database to store the transformed data. The schema is defined in the `dags/src/models.py` using SQLAlechemy ORM models, and contains three tables:

```
- cities: static table; only updated when a city is added.

- current_weather: dynamic table; updated every time the DAG runs and stores only basic data for that  specific run.

- full_weather: dynamic table; updated every time the DAG runs and stores all the data from every run.
```

### Data ERD

The following diagram represents the relationships between the tables in the database and can also be found [here](https://dbdiagram.io/d/66ea211fa0828f8aa62edb57):

![ERD for Weather data](images/weather_erd.png)


## Airflow DAG

The core logic of the ETL process is managed by an **Airflow DAG** created by `dags/weather_etl_dag.py`. The DAG is scheduled to run every hour, fetching weather data for multiple cities and inserting it into the PostgreSQL database.

### DAG Workflow:
1. Fetch current weather data from the OpenWeatherMap API.
2. Transform the data (e.g., convert temperature from Kelvin to Celsius).
3. Load the data into the PostgreSQL database.

### DAG Code

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import TaskInstance
import os
import pandas as pd
import logging

from dags.src.utils import fetch_multiple_weather_data, process_weather_response, add_cities, add_current_weather, add_full_records, get_env
from dags.src.database import get_db_conn

# configure logger
logger = logging.getLogger(__name__)

api_key,  db_url = get_env()
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
    .
    .

def transform_weather_data(ti: TaskInstance) -> None:
    """Transform weather_response dict"""
    .
    .

def load_weather_data(ti: TaskInstance) -> None:
    """Load weather dataframes into database"""
    .
    .

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

extract_task >> transform_task >> load_task

```


## Running the Project

Once the Docker containers are running, access the Airflow web UI at:

```
http://localhost:8080
```

Log in with the default credentials (`airflow`/`airflow`), and navigate to the DAGs section to trigger the `weather_etl_dag` manually or wait for it to run on schedule.

**Airflow UI with Successful DAG Execution**:
  ![Airflow DAG Success](images/weather_etl_dag_hourly.png)


## Continuous Integration and Testing

### Github Actions
The project includes a **github actions CI/CD pipeline** that ensures the following:
- **Linting, formatting and type checking**: The Python code follows PEP8 standards and is checked using pre-commit hooks (extras: ruff and mypy).
- **Automated Tests**: The project has unit tests to ensure the integrity of the ETL process, as well as test dags runs.

### Makefile
The project also consists of a Makefile with commands for setting up a test environment, running tests, and linting.

**Makefile Commands**:
```
make help           - Shows available commands
make setup          - Setup test environment and initialize Airflow
make webserver      - Start test Airflow webserver
make scheduler      - Start Airflow scheduler"
make stop           - Stop Airflow webserver and scheduler"
make test           - Run tests"
make run_pre-commit - Run pre-commit checks"
make clean			- Clear temp and test directories"
make all_checks	    - Run all checks and clean up"
```
### Logging

Airflow logs are saved to `logs/` folder and also displayed in the console and web UI. There's also an `app_log/` folder in `logs/` that stores the application logs:
- **General Log**: `/logs/app_log/weather_etl.log`
- **Error Log**: `/logs/app_log/weather_etl-error.log`


## Contributing
Contributions are welcome! Please fork the repository and submit a pull request with your proposed changes.

## Acknowledgment
This project was created as part of the [Data Epic Mentorship program](https://github.com/Data-Epic/) data engineering projects.
