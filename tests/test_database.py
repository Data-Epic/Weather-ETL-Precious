import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from dags.src.database import (
    get_db_engine,
    get_session,
    create_tables,
    drop_tables,
    find_tables,
    init_db,
    get_db_conn,
)


@pytest.fixture(scope="module")
def test_engine():
    """Fixture for creating an in-memory SQLite engine."""
    engine = create_engine("sqlite:///:memory:")
    yield engine
    engine.dispose()


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


def test_drop_tables(test_engine):
    """Test that drop_tables drops specified tables in the db."""
    create_tables(test_engine)
    tables = ["cities", "full_records", "current_weather"]
    inspector = inspect(test_engine)
    assert "cities" in inspector.get_table_names()

    drop_tables(test_engine, tables)
    inspector = inspect(test_engine)
    assert "cities" not in inspector.get_table_names()


def test_find_tables(test_engine):
    """Test that find_tables finds specified tables in db"""
    create_tables(test_engine)
    req_tables = ["cities", "full_records", "current_weather"]
    tables = find_tables(test_engine, req_tables)
    assert isinstance(tables, list)
    assert "cities" in tables


def test_init_db(test_engine):
    """Test that init_db initializes the database."""
    init_db("sqlite:///:memory:")
    inspector = inspect(test_engine)
    tables = inspector.get_table_names()
    assert "cities" in tables
    assert "full_records" in tables
    assert "current_weather" in tables


def test_get_db_conn():
    """Test that get_db_conn creates a session."""
    session = get_db_conn("sqlite:///:memory:")
    assert isinstance(session, Session)

    inspector = inspect(session.bind)
    tables = inspector.get_table_names()
    assert "cities" in tables

    session.close()
