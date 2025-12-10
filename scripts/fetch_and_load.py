# scripts/fetch_and_load.py
# scripts/fetch_and_load.py
from pathlib import Path
import sys

# ensure project root is on sys.path so "import src.*" works
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# now your existing imports can follow
from src.db import init_db, get_conn
from src.fetch_api import fetch_airport_by_iata, fetch_flights_for_airport, safe_get
from src.etl import upsert_airport, insert_flight
...
import json
from pathlib import Path
from src.db import init_db, get_conn
from src.fetch_api import fetch_airport_by_iata, fetch_flights_for_airport
from src.etl import upsert_airport, insert_flight

# initialize DB and folders
init_db()
Path("data/raw").mkdir(parents=True, exist_ok=True)

# Small list of major IATA sample airports (edit as you like)
IATAS = ["DEL","BOM","BLR","MAA","HYD","CCU","AMD","GOI"]

conn = get_conn()

for iata in IATAS:
    print("Fetching airport:", iata)
    try:
        airport_json = fetch_airport_by_iata(iata)
        # save raw
        with open(f"data/raw/{iata}_airport.json", "w") as fh:
            json.dump(airport_json, fh, indent=2)
        # upsert
        upsert_airport(conn, airport_json)
    except Exception as e:
        print("Failed airport:", iata, e)

    print("Fetching flights for:", iata)
    try:
        flights_json = fetch_flights_for_airport(iata, direction="arrivals", days=1)
        # flights_json could be dict or list depending on API; try to extract array
        flights = flights_json.get("data") if isinstance(flights_json, dict) and flights_json.get("data") else flights_json
        # some endpoints return list, some dict
        if isinstance(flights, dict):
            # try some common key
            flights = flights.get("flights") or flights.get("items") or []
        if not flights:
            print("No flights returned for", iata)
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
