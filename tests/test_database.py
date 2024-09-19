import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.engine import Engine
import pandas as pd
from dags.src.database import (
    get_db_engine,
    get_session,
    create_tables,
    drop_existing_tables,
    find_existing_tables,
    init_db,
    get_db_conn,
)


@pytest.fixture(scope="module")
def test_engine():
    """Fixture for creating an in-memory SQLite engine."""
    engine = create_engine("sqlite:///:memory:")
    yield engine
    engine.dispose()


@pytest.fixture(scope="module")
def test_session(test_engine):
    """Fixture for creating a session bound to the in-memory SQLite engine."""
    session = sessionmaker(bind=test_engine)
    yield session
    session.close()


@pytest.fixture(scope="module")
def test_db_conn():
    """Fixture for creating a session bound to the in-memory SQLite engine."""
    session = get_db_conn("sqlite:///:memory:")
    yield session
    session.close()


@pytest.fixture(scope="module")
def test_city_data():
    return pd.DataFrame(
        [
            {
                "name": "New City",
                "latitude": 1.0,
                "longitude": 1.0,
                "country": "New Country",
                "timezone_offset": 1,
                "timezone_utc": "UTC+1",
            }
        ]
    )


def test_get_db_engine():
    """Test that get_db_engine creates and returns a database engine."""
    engine = get_db_engine("sqlite:///:memory:")
    assert isinstance(engine, Engine)
    engine.dispose()


def test_get_session(test_engine):
    """Test that get_session creates and returns a new database session."""
    session = get_session(test_engine)
    assert isinstance(session, Session)
    session.close()


def test_create_tables(test_engine):
    """Test that create_tables creates the necessary tables in the db."""
    create_tables(test_engine)
    inspector = inspect(test_engine)
    tables = inspector.get_table_names()
    assert "cities" in tables
    assert "full_records" in tables
    assert "current_weather" in tables


def test_drop_existing_tables(test_engine):
    """Test that drop_existing_tables drops all existing tables in the db."""
    create_tables(test_engine)
    inspector = inspect(test_engine)
    assert "cities" in inspector.get_table_names()

    drop_existing_tables(test_engine)
    inspector = inspect(test_engine)
    assert "cities" not in inspector.get_table_names()


def test_find_existing_tables(test_engine):
    """Test that find_existing_tables in db"""
    create_tables(test_engine)
    tables = find_existing_tables(test_engine)
    assert isinstance(tables, list)
    assert "cities" in tables


def test_init_db(test_engine):
    """Test that init_db initializes the database."""
    init_db("sqlite:///:memory:")
    inspector = inspect(test_engine)
    tables = inspector.get_table_names()
    assert "cities" in tables


def test_get_db_conn():
    """Test that get_db_conn creates a session."""
    session = get_db_conn("sqlite:///:memory:")
    assert isinstance(session, Session)
    session.close()


# def test_get_city_id(test_city_data):
#     """Test that get_city_id retrieves the city ID from the database."""
#     city_df = test_city_data

#     session = get_db_conn()
#     city_id = get_city_id(session, city_df)
#     assert isinstance(city_id, int)
#     assert city_id > 0


# def test_add_city(test_session, test_city_data):
#     """Test that add_city adds a city to the database and returns the city ID."""

#     city_df = test_city_data
#     city_id = add_city(test_session, city_df)
#     assert isinstance(city_id, int)
#     assert city_id > 0

#     # Verify the city was added
#     city = test_session.query(City).filter_by(name='New City').first()
#     assert city is not None
#     assert city.id == city_id
