# streamlit_app.py
"""
Streamlit dashboard for Flight Analytics
- Uses SQLAlchemy DB from db.py (SessionLocal)
- Computes delays from scheduled/actual timestamps
- Shows top metrics, filters, airport details, delay analysis and leaderboards
"""

import os
import pathlib
import math
from datetime import datetime
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from db import SessionLocal, Airport, Aircraft, Flight, AirportDelay  # uses your db.py

load_dotenv()

st.set_page_config(page_title="Air Tracker — Flight Analytics", layout="wide")
st.title("✈️ Air Tracker — Flight Analytics")
st.markdown("Interactive dashboard for airports, flights, and delays.")

# -------------------------
# DEBUG: show DB details in sidebar
# -------------------------
DB_URL = os.getenv("DATABASE_URL", "sqlite:///flight_analytics.db")
st.sidebar.markdown("### DEBUG INFO")
st.sidebar.code(DB_URL)
if DB_URL.startswith("sqlite"):
    # resolve sqlite path
    p = pathlib.Path(DB_URL.replace("sqlite:///", "")).expanduser()
    st.sidebar.write("Resolved DB file:", str(p))
    try:
        st.sidebar.write("Exists:", p.exists())
        st.sidebar.write("Size (bytes):", p.stat().st_size if p.exists() else "N/A")
        st.sidebar.write("mtime:", p.stat().st_mtime if p.exists() else "N/A")
    except Exception as e:
        st.sidebar.write("file stat error:", e)

# also show simple table counts (safe)
try:
    engine = create_engine(DB_URL, future=True)
    with engine.connect() as conn:
        for name in ("airport", "flights", "aircraft", "airport_delays"):
            try:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {name}")).scalar_one()
                st.sidebar.write(f"{name}: {count}")
            except Exception as e:
                st.sidebar.write(f"{name}: err ({e})")
except Exception as e:
    st.sidebar.write("engine/connect error:", e)

# -------------------------
# Data loading
# -------------------------
@st.cache_data(ttl=300)
def load_dataframes():
    """
    Load DB tables into pandas DataFrames.
    Uses SessionLocal defined in db.py.
    """
    session = SessionLocal()
    try:
        df_airports = pd.read_sql(session.query(Airport).statement, session.bind) if session.query(Airport).count() >= 0 else pd.DataFrame()
        df_flights = pd.read_sql(session.query(Flight).statement, session.bind) if session.query(Flight).count() >= 0 else pd.DataFrame()
        df_aircraft = pd.read_sql(session.query(Aircraft).statement, session.bind) if session.query(Aircraft).count() >= 0 else pd.DataFrame()
        # airport_delays may be empty; load if exists
        try:
            df_delays = pd.read_sql(session.query(AirportDelay).statement, session.bind)
        except Exception:
            df_delays = pd.DataFrame()
    except Exception as e:
        # fallback to empty frames if error
        st.sidebar.write("Error loading DB into DataFrames:", e)
        df_airports = pd.DataFrame()
        df_flights = pd.DataFrame()
        df_aircraft = pd.DataFrame()
        df_delays = pd.DataFrame()
    finally:
        session.close()
    return df_airports, df_flights, df_aircraft, df_delays

df_airports, df_flights, df_aircraft, df_delays = load_dataframes()

# -------------------------
# Compute delays and derived metrics
# -------------------------
# Work on a copy
dff = df_flights.copy() if not df_flights.empty else pd.DataFrame(columns=[
    "flight_id","flight_number","aircraft_registration","origin_iata","destination_iata",
    "scheduled_departure","actual_departure","scheduled_arrival","actual_arrival","status","airline_code"
])

# Parse datetimes robustly
for col in ("scheduled_departure", "actual_departure", "scheduled_arrival", "actual_arrival"):
    if col in dff.columns:
        dff[col] = pd.to_datetime(dff[col], errors="coerce", utc=True)

# compute delay minutes (arrival & departure)
if "scheduled_arrival" in dff.columns and "actual_arrival" in dff.columns:
    dff["arrival_delay_min"] = (dff["actual_arrival"] - dff["scheduled_arrival"]).dt.total_seconds() / 60.0
else:
    dff["arrival_delay_min"] = np.nan

if "scheduled_departure" in dff.columns and "actual_departure" in dff.columns:
    dff["departure_delay_min"] = (dff["actual_departure"] - dff["scheduled_departure"]).dt.total_seconds() / 60.0
else:
    dff["departure_delay_min"] = np.nan

# By default exclude cancelled flights from delay calculations
if "status" in dff.columns:
    valid_mask = (~dff["status"].astype(str).str.lower().eq("cancelled")) & dff["arrival_delay_min"].notna()
else:
    valid_mask = dff["arrival_delay_min"].notna()

# Compute overall average arrival delay (rounded one decimal)
if valid_mask.any():
    avg_arrival_delay = round(dff.loc[valid_mask, "arrival_delay_min"].mean(), 1)
else:
    avg_arrival_delay = None

# Per-airport delay aggregates (by destination)
if not dff.empty and "destination_iata" in dff.columns:
    per_airport = (
        dff.assign(is_delayed = dff["arrival_delay_min"] > 0)
           .groupby("destination_iata", dropna=True)
           .agg(
               total_arrivals = ("flight_id", "count"),
               delayed_arrivals = ("is_delayed", "sum"),
               avg_delay_min = ("arrival_delay_min", lambda s: round(s.dropna().mean(),1))
           )
           .reset_index()
    )
else:
    per_airport = pd.DataFrame(columns=["destination_iata","total_arrivals","delayed_arrivals","avg_delay_min"])

# -------------------------
# Top-level metrics
# -------------------------
col1, col2, col3, col4 = st.columns(4)
col1.metric("Airports", len(df_airports))
col2.metric("Flights", len(df_flights))
unique_aircraft_count = int(df_flights['aircraft_registration'].nunique()) if not df_flights.empty else 0
col3.metric("Unique Aircraft", unique_aircraft_count)

if avg_arrival_delay is None or (isinstance(avg_arrival_delay, float) and math.isnan(avg_arrival_delay)):
    col4.metric("Avg Delay (min)", "N/A")
else:
    col4.metric("Avg Delay (min)", f"{avg_arrival_delay} min")

st.markdown("---")

# -------------------------
# Flight search & filter UI
# -------------------------
with st.expander("Search / Filter Flights"):
    fs1, fs2, fs3, fs4 = st.columns([2,2,2,1])
    search_number = fs1.text_input("Flight number (partial allowed)", value="")
    airline_filter = fs2.text_input("Airline code (e.g., AI, 6E)", value="")
    status_options = ["Any"] + (sorted(dff['status'].dropna().unique().tolist()) if not dff.empty else [])
    status_sel = fs3.selectbox("Status", status_options)
    date_from = fs4.date_input("Start date", value=None)
    # build filtered DataFrame
    ff = dff.copy()
    if search_number:
        ff = ff[ff['flight_number'].astype(str).str.contains(search_number, case=False, na=False)]
    if airline_filter:
        ff = ff[ff['airline_code'].astype(str).str.contains(airline_filter, case=False, na=False)]
    if status_sel and status_sel != "Any":
        ff = ff[ff['status'] == status_sel]
    # optional date filter on scheduled_departure
    if date_from is not None and hasattr(ff, "scheduled_departure"):
        try:
            # keep same timezone semantics (scheduled_departure is timezone-aware)
            date_from_dt = pd.to_datetime(date_from).tz_localize("UTC")
            ff = ff[ff["scheduled_departure"] >= date_from_dt]
        except Exception:
            pass

    # present a few useful columns
    display_cols = ["flight_id","flight_number","aircraft_registration","origin_iata","destination_iata","scheduled_departure","actual_departure","scheduled_arrival","actual_arrival","status","airline_code","arrival_delay_min"]
    cols_to_show = [c for c in display_cols if c in ff.columns]
    st.dataframe(ff[cols_to_show].sort_values("scheduled_departure", ascending=False).head(300))

st.markdown("---")

# -------------------------
# Airport details
# -------------------------
st.header("Airport Details")
left, right = st.columns([2,3])
with left:
    airport_choices = ["All"] + sorted(df_airports['iata_code'].dropna().unique().tolist())
    sel_airport = st.selectbox("Select airport (IATA)", airport_choices)
    if sel_airport != "All":
        arow = df_airports[df_airports['iata_code'] == sel_airport]
        if not arow.empty:
            a = arow.iloc[0]
            st.write(f"**{a.get('name','')}** — {a.get('city','')}, {a.get('country','')}")
            st.write(f"Timezone: {a.get('timezone','N/A')}")
            st.write(f"Coordinates: {a.get('latitude','')}, {a.get('longitude','')}")
        else:
            st.info("Airport metadata not found.")
with right:
    if sel_airport != "All":
        arrivals = dff[dff['destination_iata'] == sel_airport].sort_values("actual_arrival", ascending=False)
        departures = dff[dff['origin_iata'] == sel_airport].sort_values("actual_departure", ascending=False)
        st.subheader("Recent Arrivals")
        cols = [c for c in ["flight_number","aircraft_registration","origin_iata","scheduled_arrival","actual_arrival","status","arrival_delay_min"] if c in arrivals.columns]
        st.dataframe(arrivals[cols].head(20))
        st.subheader("Recent Departures")
        cols2 = [c for c in ["flight_number","aircraft_registration","destination_iata","scheduled_departure","actual_departure","status","departure_delay_min"] if c in departures.columns]
        st.dataframe(departures[cols2].head(20))

st.markdown("---")

# -------------------------
# Delay Analysis
# -------------------------
st.header("Delay Analysis")
if not per_airport.empty:
    top_delays = per_airport.sort_values("avg_delay_min", ascending=False).head(15)
    fig = px.bar(top_delays, x="destination_iata", y="avg_delay_min",
                 labels={"destination_iata":"Airport", "avg_delay_min":"Avg Delay (min)"})
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(per_airport.sort_values("avg_delay_min", ascending=False).head(50))
else:
    st.info("No valid delay data available. Ensure flights have scheduled and actual arrival timestamps.")

st.markdown("---")

# -------------------------
# Route leaderboards
# -------------------------
st.header("Route Leaderboards")
if not dff.empty:
    route_counts = dff.groupby(['origin_iata', 'destination_iata']).size().reset_index(name='count').sort_values("count", ascending=False).head(30)
    st.subheader("Busiest routes")
    st.dataframe(route_counts)

    # Most delayed airports (by % delayed)
    delayed = dff[dff['arrival_delay_min'] > 0].groupby('destination_iata').size().reset_index(name='delayed_count')
    arrivals = dff.groupby('destination_iata').size().reset_index(name='total_arrivals')
    merged = arrivals.merge(delayed, on='destination_iata', how='left').fillna(0)
    merged['pct_delayed'] = (merged['delayed_count'] / merged['total_arrivals'] * 100).round(1)
    st.subheader("Airports by % delayed arrivals")
    st.dataframe(merged.sort_values('pct_delayed', ascending=False).head(20))
else:
    st.info("No flight data available. Run ingestion scripts first to populate flights and aircraft.")

st.markdown("---")
st.caption("Data model based on project brief. Use the ingestion scripts to add more real/synthetic data.")

# -------------------------
# Footer / tips
# -------------------------
st.write("Tips: If numbers appear stale, run `streamlit cache clear` and restart the app. If you're using Streamlit Cloud, point DATABASE_URL to a remote Postgres for persistence.")
