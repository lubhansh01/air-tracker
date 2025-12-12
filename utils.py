# utils.py
import os
import requests
from dotenv import load_dotenv
from db import SessionLocal, Airport, Aircraft, Flight, AirportDelay
from sqlalchemy.exc import IntegrityError
import time

load_dotenv()
API_KEY = os.getenv("RAPIDAPI_KEY")
API_HOST = os.getenv("RAPIDAPI_HOST", "aerodatabox.p.rapidapi.com")
HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST
}

BASE_URL = f"https://{API_HOST}"

def get_airport_by_iata(iata):
    url = f"{BASE_URL}/airports/iata/{iata}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()

def get_flights_for_airport(iata, days=3, direction="arrival"):
    """
    direction: 'arrival' or 'departure'
    Example endpoint pattern (AeroDataBox): /flights/airports/utc/{iata}/{YYYY-MM-DD}
    We'll use the 'flights/airports' endpoint where available.
    Note: adjust to the exact AeroDataBox endpoints if they differ.
    """
    url = f"{BASE_URL}/flights/airports/icao/{iata}/2023-01-01"  # fallback placeholder
    # Many AeroDataBox endpoints require specific patterns. For reliability, try quick route:
    # Use "flights/airlines" or other endpoints depending on API docs.
    # Here we show a generalized placeholder — when you run, replace with the correct path or parameters.
    raise NotImplementedError("Replace get_flights_for_airport with actual AeroDataBox endpoint for your plan.")

def safe_insert(session, obj):
    try:
        session.add(obj)
        session.commit()
    except IntegrityError:
        session.rollback()

# convenience small wrapper to upsert flight row (replace if exists)
def upsert_flight(session, flight_obj):
    existing = session.get(Flight, flight_obj.flight_id)
    if existing:
        # update fields
        for k, v in flight_obj.__dict__.items():
            if k.startswith("_"):
                continue
            setattr(existing, k, v)
        session.commit()
    else:
        try:
            session.add(flight_obj)
            session.commit()
        except IntegrityError:
            session.rollback()
