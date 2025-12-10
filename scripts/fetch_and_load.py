# scripts/fetch_and_load.py
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.db import init_db, get_conn
from src.fetch_api import fetch_airport_by_iata, fetch_flights_for_airport
from src.etl import upsert_airport, insert_flight
import json

init_db()
Path("data/raw").mkdir(parents=True, exist_ok=True)

IATAS = ["DEL","BOM","BLR","MAA","HYD","CCU","AMD","GOI"]

conn = get_conn()

for iata in IATAS:
    print("Fetching airport:", iata)
    try:
        airport_json = fetch_airport_by_iata(iata)
        if airport_json:
            with open(f"data/raw/{iata}_airport.json", "w") as fh:
                json.dump(airport_json, fh, indent=2)
            upsert_airport(conn, airport_json)
        else:
            print("Airport endpoint returned no data for", iata)
    except Exception as e:
        print("Failed airport:", iata, e)

    print("Fetching flights for:", iata)
    try:
        flights_json = fetch_flights_for_airport(iata, direction="arrivals", days=1)
        if not flights_json:
            print("No flights endpoint data for", iata, "— skipping")
            continue

        # Normalise to a list of flight dicts:
        flights = []
        if isinstance(flights_json, list):
            flights = flights_json
        elif isinstance(flights_json, dict):
            # common keys that may contain lists
            for k in ("data", "flights", "items", "results", "response"):
                if k in flights_json and isinstance(flights_json[k], list):
                    flights = flights_json[k]
                    break
            # as fallback, try to find any list inside dict
            if not flights:
                for v in flights_json.values():
                    if isinstance(v, list):
                        flights = v
                        break

        if not flights:
            print("No flight list extracted for", iata)
            continue

        for f in flights:
            try:
                insert_flight(conn, f)
            except Exception as e:
                print("Insert flight failed:", e)
    except Exception as e:
        print("Failed flights for:", iata, e)

conn.close()
print("Done. DB path:", Path('data/airtracker.db').resolve())
