import sqlite3
from src.db import get_conn
import pandas as pd
from datetime import datetime

def upsert_airport(airport_json):
    # parse relevant fields (adjust keys to API)
    data = {
        "icao_code": airport_json.get("icao"),
        "iata_code": airport_json.get("iata"),
        "name": airport_json.get("name"),
        "city": airport_json.get("city"),
        "country": airport_json.get("country"),
        "continent": airport_json.get("continent"),
        "latitude": airport_json.get("location", {}).get("lat"),
        "longitude": airport_json.get("location", {}).get("lon"),
        "timezone": airport_json.get("timezone")
    }
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO airport (icao_code, iata_code, name, city, country, continent, latitude, longitude, timezone)
        VALUES (:icao_code, :iata_code, :name, :city, :country, :continent, :latitude, :longitude, :timezone)
    """, data)
    conn.commit()
    conn.close()

def insert_flight(flight_json):
    # map fields — these keys depend on API
    flight_id = flight_json.get("flightId") or flight_json.get("id")
    rec = {
        "flight_id": flight_id,
        "flight_number": flight_json.get("number"),
        "aircraft_registration": flight_json.get("aircraft", {}).get("registration"),
        "origin_iata": flight_json.get("departure", {}).get("iata"),
        "destination_iata": flight_json.get("arrival", {}).get("iata"),
        "scheduled_departure": flight_json.get("departure", {}).get("scheduledTime"),
        "actual_departure": flight_json.get("departure", {}).get("actualTime"),
        "scheduled_arrival": flight_json.get("arrival", {}).get("scheduledTime"),
        "actual_arrival": flight_json.get("arrival", {}).get("actualTime"),
        "status": flight_json.get("status"),
        "airline_code": flight_json.get("airline", {}).get("iata")
    }
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO flights (flight_id, flight_number, aircraft_registration,
          origin_iata, destination_iata, scheduled_departure, actual_departure,
          scheduled_arrival, actual_arrival, status, airline_code)
        VALUES (:flight_id, :flight_number, :aircraft_registration,
          :origin_iata, :destination_iata, :scheduled_departure, :actual_departure,
          :scheduled_arrival, :actual_arrival, :status, :airline_code)
    """, rec)
    conn.commit()
    conn.close()
