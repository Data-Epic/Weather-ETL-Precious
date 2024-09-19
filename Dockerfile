FROM apache/airflow:2.10.0
COPY requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

# ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
# ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
