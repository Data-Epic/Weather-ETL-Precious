export AIRFLOW_HOME:=$(shell pwd)/airflow_home
export AIRFLOW__CORE__LOAD_EXAMPLES=False
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
	@echo "  make pre-commit	- Run pre-commit checks"
	@echo "  make airflow-test	- Test Airflow DAG"
	@echo "  make pytest		- Run unit tests"
	@echo "  make test          - Run all tests"
	@echo "	 make clean			- Clear temp and test directories"
	@echo "  make all-checks	- Run all checks and clean up"


.PHONY: setup
setup:
	@echo "Setting up environment"
	python -m venv myenv
	pip install --upgrade pip
	pip install -r requirements.txt
	pip install apache-airflow==2.10.1

.PHONY: pre-commit
pre-commit:
	@echo "Running pre-commit checks"
	pre-commit install
	pre-commit
	pre-commit run --all-files -v


.PHONY: airflow-test
airflow-test: setup
	@echo "Running Airflow tests"
	mkdir -p $(AIRFLOW_HOME)/logs
	airflow db init
	echo "Running Airflow tests"
	airflow dags list
	airflow dags test weather_etl_dag_hourly
	@echo "Airflow tests passed"
	rm -rf $(AIRFLOW_HOME)



.PHONY: pytest
pytest: setup
	@echo "Running unit tests"
	pytest -s

.PHONY: test
test: pytest airflow-test


.PHONY: clean
clean:
	@echo "Cleaning environment"
	rm -rf $(AIRFLOW_HOME)
	rm -rf .ruff_cache
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf myenv


.PHONY: all-checks
all-checks: setup pre-commit test clean
