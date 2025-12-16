# streamlit_app.py
"""
Streamlit dashboard for Flight Analytics with auto-init & demo generator.

- Auto-init DB & demo data for Streamlit Cloud (ephemeral SQLite)
- Sidebar generator to create many synthetic aircraft & flights
- Computes arrival/departure delays and shows Avg Delay (min)
- Flight search/filter, airport details, delay analysis and leaderboards
- SQL Query Explorer for evaluation (11 queries)
"""

import os
import time
import pathlib
import math
import random
import string
import uuid
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# ---------------------------------------------------------------------
# Attempt to import ORM helpers if present (db.py)
# ---------------------------------------------------------------------
try:
    from db import init_db, SessionLocal, Airport, Aircraft, Flight, AirportDelay
except Exception:
    init_db = None
    SessionLocal = None
    Airport = None
    Aircraft = None
    Flight = None
    AirportDelay = None

DB_URL = os.getenv("DATABASE_URL", "sqlite:///flight_analytics.db")
engine = create_engine(DB_URL, future=True)

# ---------------------------------------------------------------------
# AUTO INIT DATABASE + DEMO DATA
# ---------------------------------------------------------------------
def auto_init_db_and_demo():
    try:
        if init_db:
            init_db()
    except Exception:
        pass

auto_init_db_and_demo()

# ---------------------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------------------
st.set_page_config(page_title="Air Tracker — Flight Analytics", layout="wide")
st.title("✈️ Air Tracker — Flight Analytics")
st.markdown("Interactive dashboard for airports, flights, delays, and SQL analytics.")

# ---------------------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_data():
    with engine.connect() as conn:
        airports = pd.read_sql("SELECT * FROM airport", conn)
        flights = pd.read_sql("SELECT * FROM flights", conn)
        aircraft = pd.read_sql("SELECT * FROM aircraft", conn)
    return airports, flights, aircraft

df_airports, df_flights, df_aircraft = load_data()

# ---------------------------------------------------------------------
# PREPROCESS FLIGHTS
# ---------------------------------------------------------------------
dff = df_flights.copy()

for col in ["scheduled_departure", "actual_departure", "scheduled_arrival", "actual_arrival"]:
    if col in dff.columns:
        dff[col] = pd.to_datetime(dff[col], errors="coerce", utc=True)

dff["arrival_delay_min"] = (
    (dff["actual_arrival"] - dff["scheduled_arrival"])
    .dt.total_seconds() / 60
)

valid_delay = dff[
    dff["arrival_delay_min"].notna() &
    (~dff["status"].str.lower().eq("cancelled"))
]

avg_delay = round(valid_delay["arrival_delay_min"].mean(), 1) if not valid_delay.empty else None

# ---------------------------------------------------------------------
# TOP METRICS
# ---------------------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)
c1.metric("Airports", len(df_airports))
c2.metric("Flights", len(df_flights))
c3.metric("Aircraft", dff["aircraft_registration"].nunique())
c4.metric("Avg Delay (min)", f"{avg_delay}" if avg_delay else "N/A")

st.markdown("---")

# ---------------------------------------------------------------------
# FLIGHT FILTER
# ---------------------------------------------------------------------
with st.expander("Search / Filter Flights"):
    f1, f2, f3, f4 = st.columns([2,2,2,1])
    fn = f1.text_input("Flight Number")
    al = f2.text_input("Airline Code")
    stt = f3.selectbox("Status", ["Any"] + sorted(dff["status"].dropna().unique()))
    date_sel = f4.date_input("Date", value=None)

    ff = dff.copy()
    if fn:
        ff = ff[ff["flight_number"].str.contains(fn, case=False, na=False)]
    if al:
        ff = ff[ff["airline_code"].str.contains(al, case=False, na=False)]
    if stt != "Any":
        ff = ff[ff["status"] == stt]
    if date_sel:
        start = pd.to_datetime(date_sel).tz_localize("UTC")
        end = start + pd.Timedelta(days=1)
        ff = ff[(ff["scheduled_departure"] >= start) & (ff["scheduled_departure"] < end)]

    st.dataframe(ff.head(300), use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------
# SQL QUERY EXPLORER (MENTOR REQUIREMENT)
# ---------------------------------------------------------------------
SQL_QUERIES = {
    "1. Flights per aircraft model": """
        SELECT a.model, COUNT(f.flight_id) AS cnt
        FROM flights f
        LEFT JOIN aircraft a ON f.aircraft_registration = a.registration
        GROUP BY a.model
        ORDER BY cnt DESC;
    """,
    "2. Aircraft assigned to more than 5 flights": """
        SELECT a.registration, a.model, COUNT(f.flight_id) AS cnt
        FROM flights f
        JOIN aircraft a ON f.aircraft_registration = a.registration
        GROUP BY a.registration, a.model
        HAVING cnt > 5;
    """,
    "3. Airports with more than 5 outbound flights": """
        SELECT ap.name, COUNT(f.flight_id) AS outbound_count
        FROM flights f
        JOIN airport ap ON ap.iata_code = f.origin_iata
        GROUP BY ap.name
        HAVING outbound_count > 5
        ORDER BY outbound_count DESC;
    """,
    "4. Top 3 destination airports by arrivals": """
        SELECT ap.name, ap.city, COUNT(f.flight_id) AS arrivals
        FROM flights f
        JOIN airport ap ON ap.iata_code = f.destination_iata
        GROUP BY ap.name, ap.city
        ORDER BY arrivals DESC
        LIMIT 3;
    """,
    "5. Domestic vs International flights": """
        SELECT f.flight_number, f.origin_iata, f.destination_iata,
        CASE WHEN o.country = d.country THEN 'Domestic' ELSE 'International' END AS route_type
        FROM flights f
        LEFT JOIN airport o ON o.iata_code = f.origin_iata
        LEFT JOIN airport d ON d.iata_code = f.destination_iata;
    """,
    "6. 5 most recent arrivals at DEL": """
        SELECT f.flight_number, f.aircraft_registration, o.name AS departure_airport, f.actual_arrival
        FROM flights f
        LEFT JOIN airport o ON o.iata_code = f.origin_iata
        WHERE f.destination_iata = 'DEL'
        ORDER BY f.actual_arrival DESC
        LIMIT 5;
    """,
    "7. Airports with no arriving flights": """
        SELECT ap.name, ap.iata_code
        FROM airport ap
        LEFT JOIN flights f ON ap.iata_code = f.destination_iata
        WHERE f.flight_id IS NULL;
    """,
    "8. Flight status count per airline": """
        SELECT airline_code,
        SUM(CASE WHEN status = 'On Time' THEN 1 ELSE 0 END) AS on_time,
        SUM(CASE WHEN status = 'Delayed' THEN 1 ELSE 0 END) AS delayed,
        SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled,
        COUNT(*) AS total
        FROM flights
        GROUP BY airline_code;
    """,
    "9. Cancelled flights with route details": """
        SELECT f.flight_number, f.aircraft_registration, o.name AS origin, d.name AS destination, f.scheduled_departure
        FROM flights f
        LEFT JOIN airport o ON o.iata_code = f.origin_iata
        LEFT JOIN airport d ON d.iata_code = f.destination_iata
        WHERE f.status = 'Cancelled'
        ORDER BY f.scheduled_departure DESC;
    """,
    "10. City routes with >2 aircraft models": """
        SELECT o.city || '-' || d.city AS route, COUNT(DISTINCT a.model) AS models_count
        FROM flights f
        JOIN airport o ON o.iata_code = f.origin_iata
        JOIN airport d ON d.iata_code = f.destination_iata
        JOIN aircraft a ON a.registration = f.aircraft_registration
        GROUP BY o.city, d.city
        HAVING models_count > 2;
    """,
    "11. Percentage of delayed arrivals per airport": """
        SELECT ap.name, ap.iata_code,
        SUM(CASE WHEN f.status = 'Delayed' THEN 1 ELSE 0 END) * 100.0 / COUNT(f.flight_id) AS pct_delayed
        FROM flights f
        JOIN airport ap ON ap.iata_code = f.destination_iata
        GROUP BY ap.name, ap.iata_code
        ORDER BY pct_delayed DESC;
    """
}

st.markdown("---")
st.header("📌 SQL Query Explorer (Evaluation Section)")

query_name = st.selectbox("Select a SQL query", list(SQL_QUERIES.keys()))
st.code(SQL_QUERIES[query_name], language="sql")

with engine.connect() as conn:
    try:
        result = pd.read_sql(SQL_QUERIES[query_name], conn)
        st.dataframe(result, use_container_width=True)
    except Exception as e:
        st.error("Query execution failed")
        st.exception(e)

st.markdown("---")
st.caption("This section validates all analytical SQL queries used in the project.")
