# data_ingest.py
"""
Verbose ingestion script for Flight Analytics.
- Creates DB tables (init_db)
- Tries to fetch airports from API (utils.get_airport_by_iata)
- If API not available or fails, inserts demo sample rows so Streamlit shows data.
- Prints progress and final table counts.
"""

import os
import time
import traceback
from dotenv import load_dotenv

load_dotenv()

from db import init_db, SessionLocal, Airport, Aircraft, Flight, AirportDelay
# utils.get_airport_by_iata may raise or return unexpected shapes; call inside try/except
try:
    from utils import get_airport_by_iata
except Exception:
    get_airport_by_iata = None

# Debug printing helper
def log(*args, **kwargs):
    print(*args, **kwargs, flush=True)

def insert_demo_data(session):
    """
    Insert a minimal set of demo airports, aircraft and flights so the dashboard shows data.
    Will try to not duplicate if same iata/registration exists.
    """
    log("Inserting demo data into DB...")
    demo_airports = [
        {"icao_code":"VIDP","iata_code":"DEL","name":"Indira Gandhi Intl","city":"New Delhi","country":"India","continent":"Asia","latitude":28.5665,"longitude":77.1031,"timezone":"Asia/Kolkata"},
        {"icao_code":"VABB","iata_code":"BOM","name":"Chhatrapati Shivaji Intl","city":"Mumbai","country":"India","continent":"Asia","latitude":19.0896,"longitude":72.8656,"timezone":"Asia/Kolkata"},
        {"icao_code":"VOBL","iata_code":"BLR","name":"Kempegowda Intl","city":"Bengaluru","country":"India","continent":"Asia","latitude":13.1986,"longitude":77.7066,"timezone":"Asia/Kolkata"}
    ]
    demo_aircraft = [
        {"registration":"VT-ABC","model":"A320","manufacturer":"Airbus","icao_type_code":"A320","owner":"DemoAir"},
        {"registration":"VT-XYZ","model":"B737","manufacturer":"Boeing","icao_type_code":"B737","owner":"DemoAir"}
    ]
    demo_flights = [
        {"flight_id":"DEMO1","flight_number":"AI101","aircraft_registration":"VT-ABC","origin_iata":"DEL","destination_iata":"BOM","scheduled_departure":"2025-12-12T08:00:00Z","actual_departure":"2025-12-12T08:05:00Z","scheduled_arrival":"2025-12-12T10:00:00Z","actual_arrival":"2025-12-12T10:10:00Z","status":"Delayed","airline_code":"AI"},
        {"flight_id":"DEMO2","flight_number":"6E202","aircraft_registration":"VT-XYZ","origin_iata":"BOM","destination_iata":"BLR","scheduled_departure":"2025-12-12T09:00:00Z","actual_departure":"2025-12-12T09:00:00Z","scheduled_arrival":"2025-12-12T11:00:00Z","actual_arrival":"2025-12-12T10:55:00Z","status":"On Time","airline_code":"6E"}
    ]

    # Upsert airports
    for a in demo_airports:
        existing = session.execute(
            f"SELECT airport_id FROM airport WHERE iata_code = :iata",
            {"iata": a["iata_code"]}
        ).fetchone()
        if existing:
            log(f"Demo airport {a['iata_code']} exists — skipping insert.")
        else:
            session.execute(
                "INSERT INTO airport (icao_code, iata_code, name, city, country, continent, latitude, longitude, timezone) VALUES (:icao_code,:iata_code,:name,:city,:country,:continent,:latitude,:longitude,:timezone)",
                a
            )
            session.commit()
            log(f"Inserted demo airport {a['iata_code']}")

    # Upsert aircraft
    for ac in demo_aircraft:
        existing = session.execute(
            "SELECT aircraft_id FROM aircraft WHERE registration = :reg",
            {"reg": ac["registration"]}
        ).fetchone()
        if existing:
            log(f"Demo aircraft {ac['registration']} exists — skipping insert.")
        else:
            session.execute(
                "INSERT INTO aircraft (registration, model, manufacturer, icao_type_code, owner) VALUES (:registration,:model,:manufacturer,:icao_type_code,:owner)",
                ac
            )
            session.commit()
            log(f"Inserted demo aircraft {ac['registration']}")

    # Upsert flights
    for f in demo_flights:
        existing = session.execute(
            "SELECT flight_id FROM flights WHERE flight_id = :fid",
            {"fid": f["flight_id"]}
        ).fetchone()
        if existing:
            log(f"Demo flight {f['flight_id']} exists — skipping insert.")
        else:
            session.execute(
                "INSERT INTO flights (flight_id, flight_number, aircraft_registration, origin_iata, destination_iata, scheduled_departure, actual_departure, scheduled_arrival, actual_arrival, status, airline_code) VALUES (:flight_id,:flight_number,:aircraft_registration,:origin_iata,:destination_iata,:scheduled_departure,:actual_departure,:scheduled_arrival,:actual_arrival,:status,:airline_code)",
                f
            )
            session.commit()
            log(f"Inserted demo flight {f['flight_id']}")

def try_fetch_airports_from_api(iata_list, session):
    """
    Try to fetch airports via utils.get_airport_by_iata.
    Returns True if at least one insert succeeded, False otherwise.
    """
    if get_airport_by_iata is None:
        log("No get_airport_by_iata function available (utils import failed). Skipping API ingestion.")
        return False

    any_inserted = False
    for iata in iata_list:
        log(f"Fetching {iata} from API...")
        try:
            data = get_airport_by_iata(iata)
            # robust extraction: handle dict values for country/continent
            def scalar(x):
                if x is None:
                    return None
                if isinstance(x, str):
                    return x
                if isinstance(x, dict):
                    for k in ("name","code"):
                        if k in x:
                            return x[k]
                    # fallback: stringify
                    return str(x)
                return str(x)

            icao = data.get("icao") or data.get("icao_code") or None
            iata_code = data.get("iata") or data.get("iata_code") or iata
            name = data.get("name") or scalar(data.get("name"))
            city = data.get("city") or scalar(data.get("city"))
            country = scalar(data.get("country"))
            continent = scalar(data.get("continent"))
            loc = data.get("location") or {}
            lat = None
            lon = None
            if isinstance(loc, dict):
                lat = loc.get("lat") or loc.get("latitude")
                lon = loc.get("lon") or loc.get("longitude")
            tz = None
            if isinstance(data.get("timezone"), dict):
                tz = data.get("timezone").get("tzName")
            else:
                tz = data.get("timezone")

            # Insert if not exists
            existing = session.execute("SELECT airport_id FROM airport WHERE iata_code = :iata", {"iata": iata_code}).fetchone()
            if existing:
                log(f"Airport {iata_code} already exists in DB — skipping.")
                any_inserted = True
                continue

            session.execute(
                "INSERT INTO airport (icao_code, iata_code, name, city, country, continent, latitude, longitude, timezone) VALUES (:icao,:iata,:name,:city,:country,:continent,:lat,:lon,:tz)",
                {"icao": icao, "iata": iata_code, "name": name, "city": city, "country": country, "continent": continent, "lat": lat, "lon": lon, "tz": tz}
            )
            session.commit()
            log(f"Inserted airport {iata_code} from API.")
            any_inserted = True
            time.sleep(0.2)  # be polite with rate limits

        except Exception as e:
            session.rollback()
            log(f"Error fetching/inserting {iata}: {e}")
            # print traceback for debugging
            traceback.print_exc()
            # continue to next airport
            continue

    return any_inserted

def print_counts(session):
    """Print counts of tables to help debug."""
    def safe_count(tbl):
        try:
            return session.execute(f"SELECT COUNT(*) FROM {tbl}").scalar_one()
        except Exception as e:
            return f"err: {e}"
    log("DB counts after ingestion:")
    for t in ["airport", "flights", "aircraft", "airport_delays"]:
        log(f"  {t}: {safe_count(t)}")

def main():
    log("Starting data_ingest.py (verbose)")
    DB_URL = os.getenv("DATABASE_URL", "sqlite:///flight_analytics.db")
    RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
    log("DATABASE_URL:", DB_URL)
    log("RAPIDAPI_KEY present? ", "Yes" if RAPIDAPI_KEY else "No")

    # Ensure DB/tables exist
    init_db()
    session = SessionLocal()

    # a small set of IATA codes to try
    IATA_LIST = ["DEL", "BOM", "BLR", "HYD", "MAA"]

    # Try API ingestion first
    inserted_from_api = False
    try:
        inserted_from_api = try_fetch_airports_from_api(IATA_LIST, session)
    except Exception as e:
        log("Exception during API ingestion:", e)
        traceback.print_exc()

    if not inserted_from_api:
        log("API ingestion did not insert rows. Falling back to demo data so Streamlit shows something.")
        insert_demo_data(session)

    # Print final counts and exit
    print_counts(session)
    session.close()
    log("Ingestion script finished.")

if __name__ == "__main__":
    main()
