import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from dags.src.models import Base, City, FullRecord, CurrentWeather


@pytest.fixture(scope="function")
def db_session():
    # Create an in-memory SQLite database
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(engine)
    engine.dispose()


def test_city_insertion(db_session):
    city = City(
        id="NG_Nkpor",
        name="Nkpor",
        latitude=6.1516,
        longitude=6.8446,
        country="NG",
        timezone_offset=3600,
        timezone_utc="UTC+1",
    )
    db_session.add(city)
    db_session.commit()

    retrieved_city = db_session.query(City).filter_by(id="NG_Nkpor").first()
    assert retrieved_city is not None
    assert retrieved_city.name == "Nkpor"
    assert retrieved_city.latitude == 6.1516
    assert retrieved_city.longitude == 6.8446


def test_weather_record_insertion(db_session):
    # First, insert a city to reference
    city = City(
        id="NG_Nkpor",
        name="Nkpor",
        latitude=6.1516,
        longitude=6.8446,
        country="NG",
        timezone_offset=3600,
        timezone_utc="UTC+1",
    )
    db_session.add(city)
    db_session.commit()

    full_record = FullRecord(
        id="NG_Nkpor_test",
        city_id=city.id,
        city_name="Nkpor",
        description="moderate rain",
        temp_k=296.94,
        temp_c=23.79,
        feels_like_k=297.83,
        temp_min_k=296.94,
        temp_max_k=296.94,
        pressure=1010,
        humidity=94,
        sea_level=1010,
        grnd_level=991,
        visibility=10000,
        wind_speed=2.66,
        wind_deg=199,
        rain_volume=2.33,
        cloud_cover=100,
        sunrise=datetime.fromtimestamp(1726550572).time(),
        sunset=datetime.fromtimestamp(1726594282).time(),
        timezone_utc="UTC+1",
    )
    db_session.add(full_record)
    db_session.commit()

    retrieved_current_weather = (
        db_session.query(FullRecord).filter_by(id="NG_Nkpor_test").first()
    )
    assert retrieved_current_weather is not None
    assert retrieved_current_weather.temp_c == 23.79
    assert retrieved_current_weather.description == "moderate rain"
    assert retrieved_current_weather.city_id == "NG_Nkpor"


def test_current_weather_insertion(db_session):
    # First, insert a city to reference
    city = City(
        id="NG_Nkpor",
        name="Nkpor",
        latitude=6.1516,
        longitude=6.8446,
        country="NG",
        timezone_offset=3600,
        timezone_utc="UTC+1",
    )
    db_session.add(city)
    db_session.commit()

    current_weather = CurrentWeather(
        id="NG_Nkpor_test",
        city_id=city.id,
        city_name="Nkpor",
        description="moderate rain",
        temp_k=296.94,
        temp_c=23.79,
        pressure=1010,
        humidity=94,
        wind_speed=2.66,
        rain_volume=2.33,
        timezone_utc="UTC+1",
    )
    db_session.add(current_weather)
    db_session.commit()

    retrieved_current_weather = (
        db_session.query(CurrentWeather).filter_by(id="NG_Nkpor_test").first()
    )
    assert retrieved_current_weather is not None
    assert retrieved_current_weather.temp_c == 23.79
    assert retrieved_current_weather.description == "moderate rain"
    assert retrieved_current_weather.city_id == "NG_Nkpor"
