from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from .models import Base
from .logger_config import info_logger, error_logger


def get_db_engine(db_url: str) -> Engine:
    """Creates and returns a new database engine."""
    try:
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        error_logger.error(f"Error creating engine: {e}", exc_info=True)
        raise e


def get_session(engine: Engine) -> Session:
    """Creates and returns a new database session."""
    try:
        SessionLocal = sessionmaker(bind=engine)
        return SessionLocal()
    except Exception as e:
        error_logger.error(f"Error creating session: {e}", exc_info=True)
        raise e


def create_tables(engine):
    """Create all tables in the database."""
    try:
        Base.metadata.create_all(engine)
        info_logger.info("Database tables created successfully.")
    except SQLAlchemyError as e:
        error_logger.error(f"Error creating tables: {e}")


def drop_tables(engine: Engine, tables: list[str]) -> None:
    """Drops a list of specified tables."""
    tables_to_drop = [Base.metadata.tables.get(table) for table in tables]
    tables_to_drop = [table for table in tables_to_drop if table is not None]
    if tables_to_drop:
        try:
            Base.metadata.drop_all(bind=engine, tables=tables_to_drop)
            info_logger.info(f"Tables {', '.join(tables)} dropped.")
        except Exception as e:
            error_logger.error(f"Error dropping tables: {e}", exc_info=True)
            raise e
    else:
        info_logger.info(f"None of the tables {', '.join(tables)} exist.")


def find_tables(engine: Engine, tables: list[str]) -> list[str]:
    """Checks if a list of tables exist in the database."""
    try:
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        found_tables = [table for table in tables if table in existing_tables]
        info_logger.info(f"Found tables: {', '.join(found_tables)}")
        return found_tables
    except Exception as e:
        error_logger.error(f"Error checking tables: {e}", exc_info=True)
        raise e


def init_db(db_url: str) -> None:
    """Initialize the database."""
    try:
        engine = get_db_engine(db_url)
        required_tables = ["cities", "full_records", "current_weather"]
        tables = find_tables(engine, required_tables)
        if tables:
            drop_tables(engine, tables)
        create_tables(engine)
    except Exception as e:
        error_logger.error(f"Error initializing database: {e}", exc_info=True)
        raise e


def get_db_conn(db_url: str) -> Session:
    """
    Connect to an existing database.
    If tables do not exist, create them.
    """
    try:
        engine = get_db_engine(db_url)
        required_tables = ["cities", "full_records", "current_weather"]
        tables = find_tables(engine, required_tables)
        for table in required_tables:
            if table not in tables:
                info_logger.info(f"Table {table} not found. Creating tables.")
                create_tables(engine)
            break
        conn = get_session(engine)
        info_logger.info("Connection to database successful.")
        return conn
    except Exception as e:
        error_logger.error(f"Error connecting to database: {e}", exc_info=True)
        raise e
