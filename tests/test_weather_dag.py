import pytest
import pendulum
from airflow.models import DagBag
from airflow.utils.state import State
from airflow import DAG
import uuid


@pytest.fixture
def dag():
    """Load the DAG from your DAG file."""
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    return dagbag.dags["weather_etl_dag_hourly"]


def test_dag_import(dag):
    """Test if the DAG is correctly imported."""
    assert dag is not None
    assert isinstance(dag, DAG)
    assert dag.dag_id == "weather_etl_dag_hourly"


def test_dag_tasks(dag):
    """Test if the DAG contains the expected tasks."""
    assert len(dag.tasks) > 0
    task_ids = [task.task_id for task in dag.tasks]
    assert "extract_weather_data" in task_ids
    assert "transform_weather_data" in task_ids
    assert "load_weather_data" in task_ids


def test_task_dependencies(dag):
    """Test if the task dependencies are correctly set up."""
    extract_task = dag.get_task("extract_weather_data")
    transform_task = dag.get_task("transform_weather_data")
    load_task = dag.get_task("load_weather_data")

    assert extract_task.downstream_task_ids == {"transform_weather_data"}
    assert transform_task.downstream_task_ids == {"load_weather_data"}
    assert load_task.downstream_task_ids == set()


def test_dag_run(dag):
    """Test if the DAG can be run manually; uses a unique run_id."""
    dag.clear()

    exc_date = pendulum.now("UTC")
    start_interval = exc_date.subtract(hours=1)
    run_id = f"manual_run_{exc_date.int_timestamp}_{uuid.uuid4()}"
    dag_run = dag.create_dagrun(
        run_id=run_id,
        execution_date=exc_date,
        start_date=exc_date,
        state=State.RUNNING,
        data_interval=(start_interval, exc_date),
    )
    assert dag_run is not None
    assert dag_run.state == State.RUNNING

    dag.clear()
    exc_date = pendulum.now("UTC")
    start_interval = exc_date.subtract(hours=1)
    run_id = f"manual_run_{exc_date.int_timestamp}_{uuid.uuid4()}"
    dag_run = dag.create_dagrun(
        run_id=run_id,
        execution_date=exc_date,
        start_date=exc_date,
        state=State.SUCCESS,
        data_interval=(start_interval, exc_date),
    )
    assert dag_run is not None
    assert dag_run.state == State.SUCCESS

    task_instances = dag_run.get_task_instances()
    task_instance_ids = [task_instance.task_id for task_instance in task_instances]
    assert "extract_weather_data" in task_instance_ids
    assert "transform_weather_data" in task_instance_ids
    assert "load_weather_data" in task_instance_ids

    dag.clear()
