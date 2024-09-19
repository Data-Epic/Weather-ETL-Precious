export AIRFLOW_HOME:=$(shell pwd)/airflow_home
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
export AIRFLOW__CORE__DAGS_FOLDER:=$(shell pwd)/dags
export DB_URL=sqlite:///:memory:
export API_KEY=8d647a23b8b7f8fe77642f3a2e739566
export PYTHONPATH:=$(shell pwd)

# Default target
.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make help          - Show this message"
	@echo "  make setup         - Setup environment"
	@echo "  make webserver     - Start Airflow webserver"
	@echo "  make scheduler     - Start Airflow scheduler"
	@echo "  make stop          - Stop Airflow webserver and scheduler"
	@echo "  make test          - Run tests"
	@echo "  make run_pre-commit- Run pre-commit checks"
	@echo "	 make clean			- Clear temp and test directories"
	@echo "  make all_checks	- Run all checks and clean up"

.PHONY: run_pre-commit
run_pre-commit:
	@echo "Running pre-commit checks"
	pre-commit install
	pre-commit
	pre-commit run --all-files -v

.PHONY: setup
setup:
	@echo "Setting up environment"
	@pip install --upgrade pip
	@pip install -r requirements.txt
	@pip install apache-airflow
	@mkdir -p $(AIRFLOW_HOME)/logs
	@airflow db init
	@airflow users create \
		--username airflow \
		--password airflow \
		--firstname First \
		--lastname Last \
		--role Admin \
		--email admin@example.com

.PHONY: webserver
webserver: setup
	@echo "Starting Airflow webserver"
	@airflow webserver -D
	@echo "Airflow webserver started. Open http://localhost:8080 in your browser"

.PHONY: scheduler
scheduler: setup
	@echo "Starting Airflow scheduler"
	@airflow scheduler -D

.PHONY: test
test: setup
	@echo "Running unit tests"
	@pytest -s

	@echo "Running Airflow tests"
	@airflow dags list
	@airflow dags test weather_etl_dag_hourly

.PHONY: stop
stop:
	@echo "Stopping Airflow webserver and scheduler"
	@pkill -f "airflow webserver"
	@pkill -f "airflow scheduler"


.PHONY: clean
clean:
	@echo "Cleaning environment"
	rm -rf $(AIRFLOW_HOME)
	rm -rf .ruff_cache
	rm -rf .pytest_cache
	rm -rf .mypy_cache

.PHONY: all_checks
all_checks: run_pre-commit setup test clean
