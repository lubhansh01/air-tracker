# scripts/import_csv.py
"""
Import flight data from a CSV file into data/airtracker.db

Usage (from project root, with venv active):
    python scripts/import_csv.py

This script defaults to:
    /Users/lubhanshsharma/Downloads/airtracker_flights.csv

If you want to use a different CSV, pass its path:
    python scripts/import_csv.py path/to/other.csv
"""
import sys
import sqlite3
from pathlib import Path
import csv

# ---------- CONFIG ----------
DEFAULT_CSV = Path("/Users/lubhanshsharma/Downloads/airtracker_flights.csv")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "airtracker.db"
SQL_SCHEMA = PROJECT_ROOT / "sql" / "create_tables.sql"
# ----------------------------

# mapping of DB column -> potential CSV header aliases (lowercased)
COLUMN_ALIASES = {
    "flight_id": ["flight_id", "id", "uniqueid", "flightid", "flight_uuid", "uid"],
    "flight_number": ["flight_number", "flightno", "flightno.", "flightnumber", "callsign", "fltno"],
    "aircraft_registration": ["aircraft_registration", "registration", "reg", "tail", "reg_number", "registration_number"],
    "origin_iata": ["origin_iata", "origin", "origin_iata_code", "from", "dep_iata", "departure_iata"],
    "destination_iata": ["destination_iata", "destination", "dest", "to", "arr_iata", "arrival_iata"],
    "scheduled_departure": ["scheduled_departure", "scheduled_dep", "dep_scheduled", "scheduleddeparture", "sched_dep"],
    "actual_departure": ["actual_departure", "actual_dep", "dep_actual", "actualdeparture"],
    "scheduled_arrival": ["scheduled_arrival", "scheduled_arr", "arr_scheduled", "scheduledarrival", "sched_arr"],
    "actual_arrival": ["actual_arrival", "actual_arr", "arr_actual", "actualarrival"],
    "status": ["status", "flight_status", "state"],
    "airline_code": ["airline_code", "carrier", "airline", "operator", "airline_iata"]
}

DB_COLUMNS_ORDER = [
    "flight_id", "flight_number", "aircraft_registration",
    "origin_iata", "destination_iata", "scheduled_departure",
    "actual_departure", "scheduled_arrival", "actual_arrival",
    "status", "airline_code"
]

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    if SQL_SCHEMA.exists():
        try:
            with open(SQL_SCHEMA, "r", encoding="utf8") as fh:
                sql = fh.read()
            conn.executescript(sql)
        except Exception as e:
            print("Warning: failed to read/exec SQL schema file:", e)
            print("Falling back to creating minimal flights table.")
            conn.executescript(_minimal_flights_sql())
    else:
        conn.executescript(_minimal_flights_sql())
    conn.commit()
    conn.close()

def _minimal_flights_sql():
    return """
    CREATE TABLE IF NOT EXISTS flights (
      flight_id TEXT PRIMARY KEY,
      flight_number TEXT,
      aircraft_registration TEXT,
      origin_iata TEXT,
      destination_iata TEXT,
      scheduled_departure TEXT,
      actual_departure TEXT,
      scheduled_arrival TEXT,
      actual_arrival TEXT,
      status TEXT,
      airline_code TEXT
    );
    """

def map_headers(csv_headers):
    # create dict mapping lowercase header -> original header
    headers_lower = {h.lower(): h for h in (csv_headers or [])}
    mapping = {}
    for db_field, aliases in COLUMN_ALIASES.items():
        found = None
        for a in aliases:
            if a.lower() in headers_lower:
                found = headers_lower[a.lower()]
                break
        mapping[db_field] = found
    return mapping

def insert_rows_from_csv(csv_path: Path):
    if not csv_path.exists():
        print("CSV not found:", csv_path)
        return

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    with open(csv_path, newline='', encoding='utf8') as fh:
        reader = csv.DictReader(fh)
        headers = reader.fieldnames or []
        mapping = map_headers(headers)

        print("Detected header mapping (db_field -> csv column):")
        for k,v in mapping.items():
            print(f"  {k:20s} -> {v}")

        # build insert SQL
        placeholders = ",".join(["?"] * len(DB_COLUMNS_ORDER))
        insert_sql = f"INSERT OR REPLACE INTO flights ({','.join(DB_COLUMNS_ORDER)}) VALUES ({placeholders})"

        inserted = 0
        with conn:
            for row in reader:
                rec = []
                for col in DB_COLUMNS_ORDER:
                    csv_col = mapping.get(col)
                    val = None
                    if csv_col:
                        # use get with fallback to different key casings
                        val = row.get(csv_col)
                        if val is None:
                            # try lower-cased header in case CSV reading changed header
                            val = row.get(csv_col.lower())
                    if val is not None:
                        val = val.strip()
                        if val == "":
                            val = None
                    rec.append(val)
                try:
                    cur.execute(insert_sql, tuple(rec))
                    inserted += 1
                except Exception as e:
                    print("Insert failed for row (skipping):", e)
        print(f"Inserted/updated {inserted} rows into {DB_PATH}")
    conn.close()

def main():
    csv_path = DEFAULT_CSV
    if len(sys.argv) > 1:
        csv_path = Path(sys.argv[1])

    print("CSV path:", csv_path)
    init_db()
    insert_rows_from_csv(csv_path)
    print("Done. You can now run your Streamlit app and refresh the dashboard.")

if __name__ == "__main__":
    main()
