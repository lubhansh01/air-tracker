# db.py
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Text, DateTime
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import IntegrityError
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///flight_analytics.db")

engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
Base = declarative_base()

class Airport(Base):
    __tablename__ = "airport"
    airport_id = Column(Integer, primary_key=True, autoincrement=True)
    icao_code = Column(String, unique=True, index=True)
    iata_code = Column(String, unique=True, index=True)
    name = Column(Text)
    city = Column(String)
    country = Column(String)
    continent = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    timezone = Column(String)

class Aircraft(Base):
    __tablename__ = "aircraft"
    aircraft_id = Column(Integer, primary_key=True, autoincrement=True)
    registration = Column(String, unique=True, index=True)
    model = Column(String)
    manufacturer = Column(String)
    icao_type_code = Column(String)
    owner = Column(String)

class Flight(Base):
    __tablename__ = "flights"
    flight_id = Column(String, primary_key=True, index=True)
    flight_number = Column(String, index=True)
    aircraft_registration = Column(String, index=True)
    origin_iata = Column(String, index=True)
    destination_iata = Column(String, index=True)
    scheduled_departure = Column(String)
    actual_departure = Column(String)
    scheduled_arrival = Column(String)
    actual_arrival = Column(String)
    status = Column(String, index=True)
    airline_code = Column(String, index=True)

class AirportDelay(Base):
    __tablename__ = "airport_delays"
    delay_id = Column(Integer, primary_key=True, autoincrement=True)
    airport_iata = Column(String, index=True)
    delay_date = Column(String)
    total_flights = Column(Integer)
    delayed_flights = Column(Integer)
    avg_delay_min = Column(Integer)
    median_delay_min = Column(Integer)
    canceled_flights = Column(Integer)

def init_db():
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    init_db()
    print("DB initialized.")
