# bulk_ingest_flights_and_aircraft.py
"""
Bulk generate aircraft + flights for airports present in the DB.

- Reads airports from the `airport` table.
- For each airport, creates `AIRCRAFT_PER_AIRPORT` synthetic aircraft (if not already present).
- For each aircraft, creates `FLIGHTS_PER_AIRCRAFT` synthetic flights to other airports in DB.
- Uses INSERT OR IGNORE to be rerunnable and safe.
- Prints progress and final counts.

Run:
    python bulk_ingest_flights_and_aircraft.py
"""

import os
import random
import string
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import uuid
import time

load_dotenv()

DB_URL = os.getenv("DATABASE_URL", "sqlite:///flight_analytics.db")
print("Using DB_URL:", DB_URL)
engine = create_engine(DB_URL, future=True)

# CONFIG: tune these for how many rows you want
AIRCRAFT_PER_AIRPORT = 4      # number of synthetic aircraft to create per airport
FLIGHTS_PER_AIRCRAFT = 6      # number of flights per aircraft to create
MAX_FLIGHT_DAYS_PAST = 7      # how many days in past to schedule flights
MAX_FLIGHT_DAYS_FUTURE = 2    # how many days in future to schedule flights

# Lists of aircraft models and airlines to randomize
AIRCRAFT_MODELS = [
    ("A320","Airbus"), ("A321","Airbus"), ("A330","Airbus"),
    ("B737","Boeing"), ("B738","Boeing"), ("B777","Boeing"),
    ("ATR72","ATR"), ("E190","Embraer")
]
AIRLINE_CODES = ["AI","6E","SG","UK","IX","G8","JA","I5"]

STATUSES = ["On Time", "Delayed", "Cancelled"]

def random_registration(prefix="VT"):
    # e.g., VT-ABC123 or VT-XYZ01
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"{prefix}-{suffix}"

def random_flight_number(airline_code):
    # e.g., AI101, 6E202 etc.
    num = random.randint(1, 999)
    return f"{airline_code}{num}"

def pick_aircraft_model():
    model, manuf = random.choice(AIRCRAFT_MODELS)
    return model, manuf

def random_time_within_range():
    # pick a scheduled departure time within past MAX_FLIGHT_DAYS_PAST to future MAX_FLIGHT_DAYS_FUTURE
    start = datetime.utcnow() - timedelta(days=MAX_FLIGHT_DAYS_PAST)
    end = datetime.utcnow() + timedelta(days=MAX_FLIGHT_DAYS_FUTURE)
    delta = end - start
    rand_seconds = random.randint(0, int(delta.total_seconds()))
    sched = start + timedelta(seconds=rand_seconds)
    # actual departure = scheduled + small variance (-10 to +60 minutes)
    variance_min = random.randint(-10, 60)
    actual = sched + timedelta(minutes=variance_min)
    # flight duration: 60 to 240 minutes
    duration_min = random.randint(60, 240)
    scheduled_arrival = sched + timedelta(minutes=duration_min)
    actual_arrival = actual + timedelta(minutes=duration_min + random.randint(-5, 30))
    return sched, actual, scheduled_arrival, actual_arrival

def iso(ts):
    # return ISO string in Z (UTC)
    return ts.replace(microsecond=0).isoformat() + "Z"

def generate_aircraft_for_airport(conn, airport_iata, n):
    created = 0
    for i in range(n):
        reg = random_registration()
        model, manuf = pick_aircraft_model()
        # Insert if not exists
        conn.execute(text(
            "INSERT OR IGNORE INTO aircraft (registration, model, manufacturer, icao_type_code, owner) "
            "VALUES (:reg, :model, :manuf, :icao, :owner)"
        ), {"reg": reg, "model": model, "manuf": manuf, "icao": model, "owner": f"Operator-{airport_iata}"})
        conn.commit()
        created += 1
        print(f"[{airport_iata}] created aircraft {reg} ({model})")
        time.sleep(0.02)
    return created

def get_existing_aircraft_for_airport(conn, owner_pattern):
    # returns list of registrations that match owner pattern (used to reuse aircraft)
    rows = conn.execute(text("SELECT registration FROM aircraft WHERE owner LIKE :pat"), {"pat": owner_pattern}).all()
    return [r[0] for r in rows]

def insert_flight(conn, flight):
    # flight is a dict of flight fields to match schema
    # Use INSERT OR IGNORE in case of duplicates
    sql = text(
        "INSERT OR IGNORE INTO flights (flight_id,flight_number,aircraft_registration,origin_iata,destination_iata,"
        "scheduled_departure,actual_departure,scheduled_arrival,actual_arrival,status,airline_code) "
        "VALUES (:flight_id,:flight_number,:aircraft_registration,:origin_iata,:destination_iata,"
        ":scheduled_departure,:actual_departure,:scheduled_arrival,:actual_arrival,:status,:airline_code)"
    )
    conn.execute(sql, flight)
    conn.commit()

def main():
    with engine.connect() as conn:
        # 1) read airports from DB
        airports = conn.execute(text("SELECT iata_code FROM airport")).all()
        airports = [r[0] for r in airports]
        if not airports:
            print("No airports found in DB. Run bulk_ingest_airports.py first.")
            return

        print(f"Found {len(airports)} airports in DB: {airports}")

        total_aircraft_created = 0
        total_flights_created = 0

        # For each airport, create aircraft and create flights for them
        for airport in airports:
            # create aircraft owned by that airport (owner string is 'Operator-<IATA>')
            owner_pattern = f"Operator-{airport}"
            # create aircraft
            created = generate_aircraft_for_airport(conn, airport, AIRCRAFT_PER_AIRPORT)
            total_aircraft_created += created

            # get list of registrations we can use for flights (include newly created)
            regs = get_existing_aircraft_for_airport(conn, f"Operator-{airport}%")
            if not regs:
                # fallback: all aircraft
                regs = [r[0] for r in conn.execute(text("SELECT registration FROM aircraft LIMIT 50")).all()]

            # create flights for each registration
            for reg in regs:
                for fnum in range(FLIGHTS_PER_AIRCRAFT):
                    # choose a random destination different from origin
                    dest = airport
                    attempt = 0
                    while dest == airport and attempt < 8:
                        dest = random.choice(airports)
                        attempt += 1
                    # build times
                    sched_dep, actual_dep, sched_arr, actual_arr = random_time_within_range()
                    airline_code = random.choice(AIRLINE_CODES)
                    flight_number = random_flight_number(airline_code)
                    # flight_id unique: use UUID4 hex or combine
                    flight_id = f"{flight_number}-{uuid.uuid4().hex[:6]}"
                    status = random.choices(STATUSES, weights=[65,25,10], k=1)[0]
                    flight = {
                        "flight_id": flight_id,
                        "flight_number": flight_number,
                        "aircraft_registration": reg,
                        "origin_iata": airport,
                        "destination_iata": dest,
                        "scheduled_departure": iso(sched_dep),
                        "actual_departure": iso(actual_dep),
                        "scheduled_arrival": iso(sched_arr),
                        "actual_arrival": iso(actual_arr),
                        "status": status,
                        "airline_code": airline_code
                    }
                    insert_flight(conn, flight)
                    total_flights_created += 1
                    print(f"  Inserted flight {flight_number} {airport}->{dest} reg={reg} status={status}")
                    # tiny sleep to avoid hammering DB
                    time.sleep(0.01)

        # print finish summary
        print("\n=== FINISHED ===")
        print("Total airports:", len(airports))
        print("Total aircraft created:", total_aircraft_created)
        print("Total flights created:", total_flights_created)

        # final counts from DB
        for t in ("airport","aircraft","flights"):
            try:
                cnt = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar_one()
                print(f"{t} count -> {cnt}")
            except Exception as e:
                print(f"{t} count error: {e}")

if __name__ == "__main__":
    main()
