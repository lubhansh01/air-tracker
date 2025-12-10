# streamlit_app.py
import os
import sqlite3
from pathlib import Path
import streamlit as st

# ---------------------------
# 1) Load secrets safely
# ---------------------------
def load_api_key():
    # Prefer st.secrets in Streamlit Cloud; fallback to environment variable (local dev)
    key = None
    try:
        # NOTE: st.secrets.get returns None if key not present
        key = st.secrets.get("AERODATABOX_API_KEY") if hasattr(st, "secrets") else None
    except Exception:
        key = None
    if not key:
        key = os.environ.get("AERODATABOX_API_KEY")
    # also make available to other modules via env var
    if key:
        os.environ["AERODATABOX_API_KEY"] = key
    return key

AERODATABOX_API_KEY = load_api_key()

# ---------------------------
# 2) DB path and init helper
# ---------------------------
DEFAULT_DB = Path("data") / "airtracker.db"
DB_PATH = None
try:
    DB_PATH = st.secrets.get("DB_PATH", str(DEFAULT_DB))
except Exception:
    DB_PATH = os.environ.get("DB_PATH", str(DEFAULT_DB))

DB_PATH = Path(DB_PATH)

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS airport (
  airport_id INTEGER PRIMARY KEY AUTOINCREMENT,
  icao_code TEXT UNIQUE,
  iata_code TEXT UNIQUE,
  name TEXT,
  city TEXT,
  country TEXT,
  continent TEXT,
  latitude REAL,
  longitude REAL,
  timezone TEXT
);

CREATE TABLE IF NOT EXISTS aircraft (
  aircraft_id INTEGER PRIMARY KEY AUTOINCREMENT,
  registration TEXT UNIQUE,
  model TEXT,
  manufacturer TEXT,
  icao_type_code TEXT,
  owner TEXT
);

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

CREATE TABLE IF NOT EXISTS airport_delays (
  delay_id INTEGER PRIMARY KEY AUTOINCREMENT,
  airport_iata TEXT,
  delay_date TEXT,
  total_flights INTEGER,
  delayed_flights INTEGER,
  avg_delay_min INTEGER,
  median_delay_min INTEGER,
  canceled_flights INTEGER
);
"""

def init_db(db_path: Path = DB_PATH):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.executescript(CREATE_TABLES_SQL)
    con.commit()
    con.close()

def get_conn():
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)

# ---------------------------
# 3) Streamlit UI
# ---------------------------
st.set_page_config(page_title="Air Tracker", layout="wide")
st.title("Air Tracker — Flight Analytics")

# show secret status
col1, col2 = st.columns(2)
with col1:
    st.metric("st.secrets AERODATABOX_API_KEY", bool(AERODATABOX_API_KEY))
with col2:
    st.metric("DB file present", DB_PATH.exists())

st.markdown("---")

# Initialize DB lazily (safe)
try:
    init_db()
except Exception as e:
    st.error("Failed to initialize database folder or file. See logs.")
    st.exception(e)
    st.stop()

# Sidebar controls
st.sidebar.header("Controls")
st.sidebar.write("This app reads data from the local SQLite DB at:")
st.sidebar.code(str(DB_PATH))
st.sidebar.caption("To populate the DB with live data, run the fetch_and_load script locally (instructions below) or upload a prepopulated DB into `data/airtracker.db` in the repo.")

# Main dashboard: counts + table preview
try:
    conn = get_conn()
    cur = conn.cursor()
    # counts
    cur.execute("SELECT COUNT(*) FROM airport")
    airports_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM flights")
    flights_count = cur.fetchone()[0]

    st.subheader("Summary")
    st.write(f"- Airports in DB: **{airports_count}**")
    st.write(f"- Flights in DB: **{flights_count}**")

    # Simple table preview if flights exist
    if flights_count > 0:
        st.subheader("Flights preview (first 100 rows)")
        df = None
        try:
            import pandas as pd
            df = pd.read_sql_query("SELECT flight_id, flight_number, origin_iata, destination_iata, status, scheduled_departure FROM flights ORDER BY scheduled_departure DESC LIMIT 100", conn)
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.write("Unable to load table preview. Install pandas in requirements if needed.")
            st.exception(e)
    else:
        st.info("No flights found in the DB yet. You can populate the DB by running the fetch script on your machine (instructions below).")

    conn.close()
except Exception as e:
    st.error("Error reading database. The DB might be corrupted or missing.")
    st.exception(e)
    st.stop()

st.markdown("---")

# Instruction block: how to populate DB (safe, user-run)
st.subheader("How to populate the DB with live data")
st.markdown(

#1. Locally (recommended for testing):
# #- Ensure you have your AERODATABOX_API_KEY set in your shell:
     ```
# export AERODATABOX_API_KEY="your_key_here"
     ```
#- Activate your venv and run the fetch script:
# #  source .venv/bin/activate   # or activate your venv
#  pip install -r requirements.txt
#  python scripts/fetch_and_load.py
     ```
#- This will create `data/airtracker.db` with tables and insert sample rows.
#2. Or upload a prepopulated `data/airtracker.db` to the repo and redeploy.
#3. On Streamlit Cloud, add your key via **Manage app → Settings → Secrets**:
