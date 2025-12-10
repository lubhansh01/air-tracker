# src/etl.py
import sqlite3
from pathlib import Path
from src.db import get_conn  # we'll create a tiny db helper below
import json
from typing import Dict, Any

def upsert_airport(conn: sqlite3.Connection, ap: Dict[str,Any]):
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO airport (icao_code, iata_code, name, city, country, continent, latitude, longitude, timezone)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        ap.get("icao"), ap.get("iata"), ap.get("name"),
        ap.get("city"), ap.get("country"),
        ap.get("continent"),
        ap.get("location", {}).get("lat"),
        ap.get("location", {}).get("lon"),
        ap.get("timezone")
    ))
    conn.commit()

def insert_flight(conn: sqlite3.Connection, flight: Dict[str,Any]):
    # map keys safely; adjust to actual API JSON shape if needed
    dep = flight.get("departure", {}) or {}
    arr = flight.get("arrival", {}) or {}
    aircraft = flight.get("aircraft", {}) or {}
    airline = flight.get("airline", {}) or {}

    rec = (
        flight.get("flightId") or flight.get("id") or flight.get("callsign") or None,
        flight.get("number") or flight.get("flightNumber") or None,
        aircraft.get("registration"),
        dep.get("iata"),
        arr.get("iata"),
        dep.get("scheduledTime"),
        dep.get("actualTime"),
        arr.get("scheduledTime"),
        arr.get("actualTime"),
        flight.get("status"),
        airline.get("iata")
    )
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO flights (
            flight_id, flight_number, aircraft_registration,
            origin_iata, destination_iata, scheduled_departure, actual_departure,
            scheduled_arrival, actual_arrival, status, airline_code
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rec)
    conn.commit()
