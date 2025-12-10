# streamlit_app.py
import os
import sqlite3
from pathlib import Path
import streamlit as st

# ---------------------------
# Safe secret loading
# ---------------------------
def load_api_key():
    """
    Return the AeroDataBox API key, preferring Streamlit secrets (st.secrets)
    and falling back to the OS environment variable.
    Also sets os.environ so other modules can read it via os.environ.
    """
    key = None
    try:
        # st.secrets may not exist locally; use .get for safety
        key = st.secrets.get("AERODATABOX_API_KEY") if hasattr(st, "secrets") else None
    except Exception:
        key = None

    if not key:
        key = os.environ.get("AERODATABOX_API_KEY")

    if key:
        os.environ["AERODATABOX_API_KEY"] = key
    return key


AERODATABOX_API_KEY = load_api_key()

# ---------------------------
# DB configuration & init
# ---------------------------
DEFAULT_DB_PATH = Path("data") / "airtracker.db"

def get_db_path():
    """
    Determine DB path: prefer st.secrets DB_PATH, then env var, otherwise DEFAULT_DB_PATH
    """
    try:
        dbp = st.secrets.get("DB_PATH") if hasattr(st, "secrets") else None
    except Exception:
        dbp = None
    if not dbp:
        dbp = os.environ.get("DB_PATH")
    return Path(dbp) if dbp else DEFAULT_DB_PATH

DB_PATH = get_db_path()

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

def init_db(path: Path = DB_PATH):
    """
    Ensure DB folder exists and create tables if DB is missing/corrupt.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    cur = conn.cursor()
    cur.executescript(CREATE_TABLES_SQL)
    conn.commit()
    conn.close()

def get_conn(path: Path = DB_PATH):
    return sqlite3.connect(str(path), check_same_thread=False)


# ---------------------------
# Streamlit app UI
# ---------------------------
st.set_page_config(page_title="Air Tracker", layout="wide")
st.title("Air Tracker — Flight Analytics")

# Top-line status metrics
col1, col2, col3 = st.columns([1,1,1])
with col1:
    st.metric("API key present", bool(AERODATABOX_API_KEY))
with col2:
    st.metric("DB file exists", DB_PATH.exists())
with col3:
    # show readable DB path
    st.write("DB path")
    st.code(str(DB_PATH))

st.markdown("---")

# Initialize DB safely
try:
    init_db()
except Exception as e:
    st.error("Failed to initialize database. Check permissions or DB path.")
    st.exception(e)
    st.stop()

# Sidebar instructions and controls
st.sidebar.header("Controls & notes")
st.sidebar.write("This app is read-only by default and uses a local SQLite DB.")
st.sidebar.write("To populate the DB, either run the fetch script locally or upload a pre-populated `data/airtracker.db` to the repo.")

# Main dashboard area
try:
    conn = get_conn()
    cur = conn.cursor()

    # summary counts
    cur.execute("SELECT COUNT(*) FROM airport")
    airports_count = cur.fetchone()[0] or 0
    cur.execute("SELECT COUNT(*) FROM flights")
    flights_count = cur.fetchone()[0] or 0

    st.subheader("Database summary")
    st.write(f"- Airports in DB: **{airports_count}**")
    st.write(f"- Flights in DB: **{flights_count}**")

    # show flight preview if available
    if flights_count > 0:
        st.subheader("Flights preview (up to 100 rows)")
        try:
            import pandas as pd
            df = pd.read_sql_query(
                "SELECT flight_id, flight_number, origin_iata, destination_iata, status, scheduled_departure FROM flights ORDER BY scheduled_departure DESC LIMIT 100",
                conn
            )
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.warning("Couldn't render DataFrame preview. Is pandas installed in requirements.txt?")
            st.exception(e)
    else:
        st.info("No flights found in DB. Use the instructions below to populate the DB.")

    conn.close()
except Exception as e:
    st.error("Error reading database. The DB may be corrupt or inaccessible.")
    st.exception(e)
    st.stop()

st.markdown("---")

# Clear, safe instructions for populating DB (no unclosed quotes, no code fences inside)
st.subheader("How to populate the DB with live data")
st.markdown(
    """
1) Local (recommended)
   - Set your AeroDataBox API key in your shell (local testing):
     export AERODATABOX_API_KEY="your_key_here"
   - Activate your virtual environment and install dependencies:
     source .venv/bin/activate   # or the command you use to activate your venv
     pip install -r requirements.txt
   - Run the fetch script which will create and populate `data/airtracker.db`:
     python scripts/fetch_and_load.py

2) Upload a pre-populated DB
   - Create `data/airtracker.db` locally, commit it to your repo (small snapshot), and redeploy.

3) Streamlit Cloud deployment
   - In Streamlit Cloud (share.streamlit.io) go to your app -> Manage app -> Settings -> Secrets and add:
     AERODATABOX_API_KEY = "your_key_here"
     DB_PATH = "data/airtracker.db"   # optional if using default
   - Then redeploy the app via the Streamlit Cloud dashboard.
"""
)

st.markdown("---")

# Quick live test action (explicit and user-triggered; does not run automatically)
st.subheader("Manual API test (click to run once)")
if st.button("Run single API test (requires key)"):
    if not AERODATABOX_API_KEY:
        st.error("No API key available. Add it to st.secrets or set AERODATABOX_API_KEY in your environment.")
    else:
        st.info("Attempting a single minimal API call (will not populate DB).")
        try:
            import requests
            headers = {
                "x-rapidapi-key": os.environ.get("AERODATABOX_API_KEY"),
                "x-rapidapi-host": "aerodatabox.p.rapidapi.com"
            }
            url = "https://aerodatabox.p.rapidapi.com/airports/iata/DEL"
            r = requests.get(url, headers=headers, timeout=15)
            r.raise_for_status()
            st.success("API call succeeded (trimmed response shown).")
            js = r.json()
            # Show a small trimmed JSON for inspection
            trimmed = {k: js.get(k) for k in list(js.keys())[:10]}
            st.json(trimmed)
        except Exception as e:
            st.error("API call failed.")
            st.exception(e)

st.markdown("---")
st.caption("If you'd like, I can provide the scripts/fetch_and_load.py, src/fetch_api.py and src/etl.py files so you can populate the DB locally. Ask me and I'll paste them.")
