from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from dags.src.models import Base
from dags.src.logger_config import info_logger, error_logger


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


def drop_existing_tables(engine: Engine) -> None:
    """Drops all existing tables."""
    try:
        Base.metadata.drop_all(bind=engine)
        info_logger.info("All existing tables dropped.")
    except Exception as e:
        error_logger.error(f"Error dropping tables: {e}", exc_info=True)
        raise e


def find_existing_tables(engine: Engine) -> list[str]:
    """Check if tables exist. returns them if they do."""
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        info_logger.info(f"Existing tables: {tables}")
        return tables
    except Exception as e:
        error_logger.error(f"Error checking tables: {e}", exc_info=True)
        raise e


def init_db(db_url: str) -> None:
    """Initialize the database."""
    try:
        engine = get_db_engine(db_url)
        tables = find_existing_tables(engine)
        if tables:
            drop_existing_tables(engine)
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
        tables = find_existing_tables(engine)
        if not tables:
            info_logger.info("No existing tables found. Creating tables.")
            create_tables(engine)
        return get_session(engine)
    except Exception as e:
        error_logger.error(f"Error connecting to database: {e}", exc_info=True)
        raise e
