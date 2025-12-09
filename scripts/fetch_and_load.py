from src.db import init_db
from src.fetch_api import safe_get, fetch_airport_by_iata, fetch_flights_for_airport
from src.etl import upsert_airport, insert_flight
import json, os
from pathlib import Path

init_db()

# list of airports (choose 10–15)
IATAS = ["DEL","BOM","BLR","MAA","HYD","CCU","AMD","CJB","IXC","GOI"]

for iata in IATAS:
    print("Fetching airport", iata)
    airport = safe_get(fetch_airport_by_iata, iata)
    upsert_airport(airport)
    # save raw
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    with open(f"data/raw/{iata}_airport.json","w") as f:
        json.dump(airport, f, indent=2)

    print("Fetching flights for", iata)
    flights_json = safe_get(fetch_flights_for_airport, iata, direction="arrivals", days=3)
    # flights_json might be list or dict
    flights_list = flights_json.get("data") if isinstance(flights_json, dict) else flights_json
    for flight in flights_list:
        try:
            insert_flight(flight)
        except Exception as e:
            print("Failed to insert flight", e)
