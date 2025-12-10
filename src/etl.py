# src/etl.py
import sqlite3
from typing import Dict, Any

def _safe_get_float(obj: dict, *path, default=None):
    """Traverse nested dict keys and try to coerce to float or return default."""
    cur = obj
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    try:
        if cur is None:
            return default
        return float(cur)
    except Exception:
        return default

def _safe_get_str(obj: dict, *path, default=None):
    cur = obj
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    if cur is None:
        return default
    return str(cur)

def upsert_airport(conn: sqlite3.Connection, ap: Dict[str, Any]):
    """
    Accepts multiple possible airport JSON shapes and extracts best-effort fields.
    """
    cur = conn.cursor()
    try:
        icao = _safe_get_str(ap, "icao") or _safe_get_str(ap, "icaoCode") or _safe_get_str(ap, "ident")
        iata = _safe_get_str(ap, "iata") or _safe_get_str(ap, "iataCode")
        name = _safe_get_str(ap, "name") or _safe_get_str(ap, "airportName")
        city = _safe_get_str(ap, "city")
        country = _safe_get_str(ap, "country")
        continent = _safe_get_str(ap, "continent")
        # latitude/longitude may be nested in different keys
        lat = _safe_get_float(ap, "location", "lat",
                              ) or _safe_get_float(ap, "position", "latitude") \
            or _safe_get_float(ap, "lat")
        lon = _safe_get_float(ap, "location", "lon",
                              ) or _safe_get_float(ap, "position", "longitude") \
            or _safe_get_float(ap, "lon")
        timezone = _safe_get_str(ap, "timezone") or _safe_get_str(ap, "timeZone")
        cur.execute("""
            INSERT OR IGNORE INTO airport (icao_code, iata_code, name, city, country, continent, latitude, longitude, timezone)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (icao, iata, name, city, country, continent, lat, lon, timezone))
        conn.commit()
    except Exception as e:
        # bubble up a descriptive error; caller will log it
        raise RuntimeError(f"upsert_airport failed: {e}")

def insert_flight(conn: sqlite3.Connection, flight: Dict[str, Any]):
    """
    Insert/replace a flight record. This mapping is tolerant to many JSON shapes.
    """
    cur = conn.cursor()
    # common nested shapes
    dep = flight.get("departure") or flight.get("departureTime") or {}
    arr = flight.get("arrival") or flight.get("arrivalTime") or {}
    aircraft = flight.get("aircraft") or flight.get("plane") or {}
    airline = flight.get("airline") or flight.get("carrier") or {}

    # Many APIs use different keys; try multiple fallbacks
    flight_id = flight.get("flightId") or flight.get("id") or flight.get("callsign") or flight.get("hexId") or flight.get("uniqueId")
    flight_number = flight.get("number") or flight.get("flightNumber") or flight.get("callsign")
    aircraft_registration = aircraft.get("registration") or aircraft.get("reg") or flight.get("registration")
    origin_iata = (dep.get("iata") or dep.get("iataCode") or dep.get("origin") or flight.get("origin"))
    destination_iata = (arr.get("iata") or arr.get("iataCode") or arr.get("destination") or flight.get("destination"))
    scheduled_departure = dep.get("scheduledTime") or dep.get("scheduled") or dep.get("scheduledLocal") or None
    actual_departure = dep.get("actualTime") or dep.get("actual") or None
    scheduled_arrival = arr.get("scheduledTime") or arr.get("scheduled") or None
    actual_arrival = arr.get("actualTime") or arr.get("actual") or None
    status = flight.get("status") or flight.get("flightStatus") or None
    airline_code = airline.get("iata") or airline.get("icao") or airline.get("code")

    # safe insert — ensure no unsupported types (convert to str when unsure)
    def _to_safe(v):
        if v is None:
            return None
        if isinstance(v, (str, int, float)):
            return str(v) if not isinstance(v, str) else v
        # last resort
        return str(v)

    rec = (
        _to_safe(flight_id),
        _to_safe(flight_number),
        _to_safe(aircraft_registration),
        _to_safe(origin_iata),
        _to_safe(destination_iata),
        _to_safe(scheduled_departure),
        _to_safe(actual_departure),
        _to_safe(scheduled_arrival),
        _to_safe(actual_arrival),
        _to_safe(status),
        _to_safe(airline_code),
    )
    try:
        cur.execute("""
            INSERT OR REPLACE INTO flights (
                flight_id, flight_number, aircraft_registration,
                origin_iata, destination_iata, scheduled_departure, actual_departure,
                scheduled_arrival, actual_arrival, status, airline_code
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rec)
        conn.commit()
    except Exception as e:
        raise RuntimeError(f"insert_flight failed: {e}")