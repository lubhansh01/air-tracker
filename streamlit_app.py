# streamlit_app.py
"""
Streamlit dashboard for Flight Analytics with auto-init & demo generator.

- Auto-init DB & demo data for Streamlit Cloud (ephemeral SQLite)
- Sidebar generator to create many synthetic aircraft & flights
- Computes arrival/departure delays and shows Avg Delay (min)
- Flight search/filter, airport details, delay analysis and leaderboards
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
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

# ---------------------------------------------------------------------
# Attempt to import ORM helpers if present (db.py). If not present we
# gracefully fallback to raw SQL via engine.
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

# DB URL and engine
DB_URL = os.getenv("DATABASE_URL", "sqlite:///flight_analytics.db")
engine = create_engine(DB_URL, future=True)

# ---------------------------------------------------------------------
# Auto-init DB & demo rows (idempotent). Safe to run on Streamlit Cloud.
# ---------------------------------------------------------------------
def _insert_demo_rows(conn, dialect_name):
    """Insert a small demo dataset (airports, aircraft, flights) idempotently."""
    airports = [
        ("VIDP","DEL","Indira Gandhi Intl","New Delhi","India","Asia",28.5665,77.1031,"Asia/Kolkata"),
        ("VABB","BOM","Chhatrapati Shivaji Intl","Mumbai","India","Asia",19.0896,72.8656,"Asia/Kolkata"),
        ("VOBL","BLR","Kempegowda Intl","Bengaluru","India","Asia",13.1986,77.7066,"Asia/Kolkata"),
        ("VOHS","HYD","Rajiv Gandhi Intl","Hyderabad","India","Asia",17.2403,78.4294,"Asia/Kolkata"),
        ("VOMM","MAA","Chennai Intl","Chennai","India","Asia",12.9941,80.1709,"Asia/Kolkata"),
        ("VECC","CCU","Netaji Subhash Ch Bose Intl","Kolkata","India","Asia",22.6547,88.4467,"Asia/Kolkata"),
        ("VOCI","COK","Cochin Intl","Kochi","India","Asia",10.1520,76.4019,"Asia/Kolkata"),
        ("VOTV","TRV","Trivandrum Intl","Thiruvananthapuram","India","Asia",8.4824,76.9204,"Asia/Kolkata"),
        ("VOGO","GOI","Dabolim","Goa","India","Asia",15.3808,73.83,"Asia/Kolkata"),
        ("VAAH","AMD","Sardar Vallabhbhai Patel Intl","Ahmedabad","India","Asia",23.0779,72.6347,"Asia/Kolkata"),
        ("VAPO","PNQ","Pune Intl","Pune","India","Asia",18.5820,73.9197,"Asia/Kolkata"),
        ("VIJR","IXJ","Jammu Airport","Jammu","India","Asia",32.6906,74.8375,"Asia/Kolkata"),
        ("VIBR","SXR","Sheikh ul-Alam Intl","Srinagar","India","Asia",33.9871,74.7749,"Asia/Kolkata"),
        ("VICG","IXC","Chandigarh Airport","Chandigarh","India","Asia",30.6735,76.7885,"Asia/Kolkata"),
        ("VAGO","GOM","Gorakhpur Airport","Gorakhpur","India","Asia",26.7614,83.4494,"Asia/Kolkata")
    ]

    demo_aircraft = [
        ("VT-AAA","A320","Airbus","A320","DemoAir"),
        ("VT-BBB","B737","Boeing","B737","DemoAir"),
        ("VT-CCC","A321","Airbus","A321","DemoAir"),
        ("VT-DDD","E190","Embraer","E190","DemoAir"),
        ("VT-EEE","ATR72","ATR","ATR72","DemoAir")
    ]

    demo_flights = [
        ("DEMO-AI101","AI101","VT-AAA","DEL","BOM","2025-12-12T08:00:00Z","2025-12-12T08:05:00Z","2025-12-12T10:00:00Z","2025-12-12T10:10:00Z","Delayed","AI"),
        ("DEMO-6E202","6E202","VT-BBB","BOM","BLR","2025-12-12T09:00:00Z","2025-12-12T09:00:00Z","2025-12-12T11:00:00Z","2025-12-12T10:55:00Z","On Time","6E")
    ]

    if dialect_name == "postgresql":
        airport_sql = (
            "INSERT INTO airport (icao_code,iata_code,name,city,country,continent,latitude,longitude,timezone) "
            "VALUES (:icao,:iata,:name,:city,:country,:continent,:lat,:lon,:tz) "
            "ON CONFLICT (iata_code) DO NOTHING"
        )
        aircraft_sql = (
            "INSERT INTO aircraft (registration,model,manufacturer,icao_type_code,owner) "
            "VALUES (:reg,:model,:manuf,:icao,:owner) "
            "ON CONFLICT (registration) DO NOTHING"
        )
        flights_sql = (
            "INSERT INTO flights (flight_id,flight_number,aircraft_registration,origin_iata,destination_iata,"
            "scheduled_departure,actual_departure,scheduled_arrival,actual_arrival,status,airline_code) "
            "VALUES (:flight_id,:flight_number,:aircraft_registration,:origin_iata,:destination_iata,"
            ":scheduled_departure,:actual_departure,:scheduled_arrival,:actual_arrival,:status,:airline_code) "
            "ON CONFLICT (flight_id) DO NOTHING"
        )
    else:
        airport_sql = (
            "INSERT OR IGNORE INTO airport (icao_code,iata_code,name,city,country,continent,latitude,longitude,timezone) "
            "VALUES (:icao,:iata,:name,:city,:country,:continent,:lat,:lon,:tz)"
        )
        aircraft_sql = (
            "INSERT OR IGNORE INTO aircraft (registration,model,manufacturer,icao_type_code,owner) "
            "VALUES (:reg,:model,:manuf,:icao,:owner)"
        )
        flights_sql = (
            "INSERT OR IGNORE INTO flights (flight_id,flight_number,aircraft_registration,origin_iata,destination_iata,"
            "scheduled_departure,actual_departure,scheduled_arrival,actual_arrival,status,airline_code) "
            "VALUES (:flight_id,:flight_number,:aircraft_registration,:origin_iata,:destination_iata,"
            ":scheduled_departure,:actual_departure,:scheduled_arrival,:actual_arrival,:status,:airline_code)"
        )

    for icao,iata,name,city,country,continent,lat,lon,tz in airports:
        try:
            conn.execute(text(airport_sql), {"icao":icao,"iata":iata,"name":name,"city":city,"country":country,"continent":continent,"lat":lat,"lon":lon,"tz":tz})
        except Exception:
            pass

    for reg,model,manuf,icao_type,owner in demo_aircraft:
        try:
            conn.execute(text(aircraft_sql), {"reg":reg,"model":model,"manuf":manuf,"icao":icao_type,"owner":owner})
        except Exception:
            pass

    for flight in demo_flights:
        try:
            params = {"flight_id": flight[0], "flight_number": flight[1], "aircraft_registration": flight[2],
                      "origin_iata": flight[3], "destination_iata": flight[4],
                      "scheduled_departure": flight[5], "actual_departure": flight[6],
                      "scheduled_arrival": flight[7], "actual_arrival": flight[8],
                      "status": flight[9], "airline_code": flight[10]}
            conn.execute(text(flights_sql), params)
        except Exception:
            pass

def auto_init_db_and_demo():
    """Create schema (via init_db if available) and insert demo rows when DB empty/missing."""
    try:
        if init_db:
            init_db()
    except Exception:
        pass

    try:
        with engine.begin() as conn:
            tables = []
            dialect = engine.dialect.name
            try:
                rows = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).all()
                tables = [r[0] for r in rows]
                dialect = "sqlite"
            except Exception:
                try:
                    rows = conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname='public'")).all()
                    tables = [r[0] for r in rows]
                    dialect = "postgresql"
                except Exception:
                    tables = []

            need_demo = False
            if "airport" not in tables or "flights" not in tables:
                need_demo = True
            else:
                try:
                    a_cnt = conn.execute(text("SELECT COUNT(*) FROM airport")).scalar_one()
                    f_cnt = conn.execute(text("SELECT COUNT(*) FROM flights")).scalar_one()
                    if (not a_cnt) or (not f_cnt):
                        need_demo = True
                except Exception:
                    need_demo = True

            if need_demo:
                _insert_demo_rows(conn, dialect)
                time.sleep(0.2)
    except Exception:
        pass

# run auto init (safe)
auto_init_db_and_demo()

# ---------------------------------------------------------------------
# Sidebar: debug info and demo generator controls
# ---------------------------------------------------------------------
st.set_page_config(page_title="Air Tracker — Flight Analytics", layout="wide")
st.title("✈️ Air Tracker — Flight Analytics")
st.markdown("Interactive dashboard for airports, flights, and delays.")

st.sidebar.markdown("### DEBUG INFO")
st.sidebar.code(DB_URL)
if DB_URL.startswith("sqlite"):
    p = pathlib.Path(DB_URL.replace("sqlite:///","")).expanduser()
    st.sidebar.write("Resolved DB file:", str(p))
    try:
        st.sidebar.write("Exists:", p.exists())
        st.sidebar.write("Size (bytes):", p.stat().st_size if p.exists() else "N/A")
        st.sidebar.write("mtime:", p.stat().st_mtime if p.exists() else "N/A")
    except Exception as e:
        st.sidebar.write("file stat error:", e)

# show counts via engine
try:
    with engine.connect() as conn:
        for name in ("airport","flights","aircraft","airport_delays"):
            try:
                cnt = conn.execute(text(f"SELECT COUNT(*) FROM {name}")).scalar_one()
                st.sidebar.write(f"{name}: {cnt}")
            except Exception as e:
                st.sidebar.write(f"{name}: err ({e})")
except Exception as e:
    st.sidebar.write("engine/connect error:", e)

# -------------------------
# Demo generator controls (sidebar)
# -------------------------
st.sidebar.markdown("### Demo data generator")
AIRCRAFT_PER_AIRPORT = st.sidebar.number_input("Aircraft to create per airport", min_value=1, max_value=20, value=4, step=1)
FLIGHTS_PER_AIRCRAFT = st.sidebar.number_input("Flights per aircraft", min_value=1, max_value=50, value=6, step=1)
REUSE_EXISTING = st.sidebar.checkbox("Reuse existing aircraft (don't create new ones if present)", value=False)
GENERATE_BTN = st.sidebar.button("Generate demo fleet & flights")

# helper small functions
def _random_registration(prefix="VT"):
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"{prefix}-{suffix}"

def _random_flight_number(airline_codes=("AI","6E","SG","IX","G8")):
    return f"{random.choice(airline_codes)}{random.randint(10,999)}"

def _iso(ts):
    return ts.replace(microsecond=0).isoformat() + "Z"

def _insert_many(engine, aircraft_per_airport, flights_per_aircraft, reuse_existing=False):
    created_aircraft = 0
    created_flights = 0
    dialect = engine.dialect.name

    if dialect == "postgresql":
        aircraft_insert_sql = text(
            "INSERT INTO aircraft (registration,model,manufacturer,icao_type_code,owner) "
            "VALUES (:reg,:model,:manuf,:icao,:owner) ON CONFLICT (registration) DO NOTHING"
        )
        flight_insert_sql = text(
            "INSERT INTO flights (flight_id,flight_number,aircraft_registration,origin_iata,destination_iata,"
            "scheduled_departure,actual_departure,scheduled_arrival,actual_arrival,status,airline_code) "
            "VALUES (:flight_id,:flight_number,:aircraft_registration,:origin_iata,:destination_iata,"
            ":scheduled_departure,:actual_departure,:scheduled_arrival,:actual_arrival,:status,:airline_code) "
            "ON CONFLICT (flight_id) DO NOTHING"
        )
    else:
        aircraft_insert_sql = text(
            "INSERT OR IGNORE INTO aircraft (registration,model,manufacturer,icao_type_code,owner) "
            "VALUES (:reg,:model,:manuf,:icao,:owner)"
        )
        flight_insert_sql = text(
            "INSERT OR IGNORE INTO flights (flight_id,flight_number,aircraft_registration,origin_iata,destination_iata,"
            "scheduled_departure,actual_departure,scheduled_arrival,actual_arrival,status,airline_code) "
            "VALUES (:flight_id,:flight_number,:aircraft_registration,:origin_iata,:destination_iata,"
            ":scheduled_departure,:actual_departure,:scheduled_arrival,:actual_arrival,:status,:airline_code)"
        )

    with engine.begin() as conn:
        rows = conn.execute(text("SELECT iata_code FROM airport")).all()
        airports = [r[0] for r in rows] if rows else []
        if not airports:
            return created_aircraft, created_flights

        for airport in airports:
            owner_prefix = f"Operator-{airport}"
            existing_regs = []
            if reuse_existing:
                existing_rows = conn.execute(text("SELECT registration FROM aircraft WHERE owner LIKE :owner"), {"owner": f"{owner_prefix}%"}).all()
                existing_regs = [r[0] for r in existing_rows]

            regs_for_airport = existing_regs.copy()
            to_create = max(0, int(aircraft_per_airport) - len(existing_regs))
            for i in range(to_create):
                registration = _random_registration(prefix="VT")
                model = random.choice(["A320","B737","A321","E190","ATR72"])
                manuf = "Airbus" if model.startswith("A3") or model.startswith("A") else ("Boeing" if model.startswith("B7") else "Other")
                try:
                    conn.execute(aircraft_insert_sql, {"reg":registration,"model":model,"manuf":manuf,"icao":model,"owner":owner_prefix})
                    created_aircraft += 1
                    regs_for_airport.append(registration)
                except Exception:
                    pass

            if not regs_for_airport:
                rows_any = conn.execute(text("SELECT registration FROM aircraft LIMIT 50")).all()
                regs_for_airport = [r[0] for r in rows_any] if rows_any else []

            for reg in regs_for_airport:
                for _ in range(int(flights_per_aircraft)):
                    dest = airport
                    attempts = 0
                    while dest == airport and attempts < 8:
                        dest = random.choice(airports)
                        attempts += 1
                    start = datetime.utcnow() - timedelta(days=7)
                    end = datetime.utcnow() + timedelta(days=2)
                    rand_seconds = random.randint(0, int((end-start).total_seconds()))
                    sched = start + timedelta(seconds=rand_seconds)
                    variance = random.randint(-10,60)
                    actual_dep = sched + timedelta(minutes=variance)
                    duration_min = random.randint(60,240)
                    sched_arr = sched + timedelta(minutes=duration_min)
                    actual_arr = actual_dep + timedelta(minutes=duration_min + random.randint(-5,30))
                    airline_code = random.choice(["AI","6E","SG","IX","G8","UK","I5"])
                    flight_number = _random_flight_number((airline_code,))
                    flight_id = f"{flight_number}-{uuid.uuid4().hex[:6]}"
                    status = random.choices(["On Time","Delayed","Cancelled"], weights=[65,25,10], k=1)[0]
                    params = {
                        "flight_id": flight_id,
                        "flight_number": flight_number,
                        "aircraft_registration": reg,
                        "origin_iata": airport,
                        "destination_iata": dest,
                        "scheduled_departure": _iso(sched),
                        "actual_departure": _iso(actual_dep),
                        "scheduled_arrival": _iso(sched_arr),
                        "actual_arrival": _iso(actual_arr),
                        "status": status,
                        "airline_code": airline_code
                    }
                    try:
                        conn.execute(flight_insert_sql, params)
                        created_flights += 1
                    except Exception:
                        pass

    return created_aircraft, created_flights

# run generation on button press
if GENERATE_BTN:
    st.sidebar.info(f"Generating {AIRCRAFT_PER_AIRPORT} aircraft per airport and {FLIGHTS_PER_AIRCRAFT} flights per aircraft...")
    ac, fl = _insert_many(engine, AIRCRAFT_PER_AIRPORT, FLIGHTS_PER_AIRCRAFT, reuse_existing=REUSE_EXISTING)
    try:
        st.cache_data.clear()
    except Exception:
        pass
    st.sidebar.success(f"Inserted ~{ac} aircraft and ~{fl} flights.")
    st.experimental_rerun()

# ---------------------------------------------------------------------
# Load dataframes (cached) - prefer ORM if SessionLocal available
# ---------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_dataframes():
    df_airports = pd.DataFrame()
    df_flights = pd.DataFrame()
    df_aircraft = pd.DataFrame()
    df_delays = pd.DataFrame()
    try:
        if SessionLocal is not None and Airport is not None:
            session = SessionLocal()
            try:
                df_airports = pd.read_sql(session.query(Airport).statement, session.bind) if Airport is not None else pd.DataFrame()
                df_flights = pd.read_sql(session.query(Flight).statement, session.bind) if Flight is not None else pd.DataFrame()
                df_aircraft = pd.read_sql(session.query(Aircraft).statement, session.bind) if Aircraft is not None else pd.DataFrame()
                try:
                    df_delays = pd.read_sql(session.query(AirportDelay).statement, session.bind) if AirportDelay is not None else pd.DataFrame()
                except Exception:
                    df_delays = pd.DataFrame()
            finally:
                session.close()
        else:
            with engine.connect() as conn:
                try:
                    df_airports = pd.read_sql(text("SELECT * FROM airport"), conn)
                except Exception:
                    df_airports = pd.DataFrame()
                try:
                    df_flights = pd.read_sql(text("SELECT * FROM flights"), conn)
                except Exception:
                    df_flights = pd.DataFrame()
                try:
                    df_aircraft = pd.read_sql(text("SELECT * FROM aircraft"), conn)
                except Exception:
                    df_aircraft = pd.DataFrame()
                try:
                    df_delays = pd.read_sql(text("SELECT * FROM airport_delays"), conn)
                except Exception:
                    df_delays = pd.DataFrame()
    except Exception:
        df_airports = pd.DataFrame()
        df_flights = pd.DataFrame()
        df_aircraft = pd.DataFrame()
        df_delays = pd.DataFrame()
    return df_airports, df_flights, df_aircraft, df_delays

df_airports, df_flights, df_aircraft, df_delays = load_dataframes()

# Prepare flights DF copy and compute delays
dff = df_flights.copy() if not df_flights.empty else pd.DataFrame(columns=[
    "flight_id","flight_number","aircraft_registration","origin_iata","destination_iata",
    "scheduled_departure","actual_departure","scheduled_arrival","actual_arrival","status","airline_code"
])

for col in ("scheduled_departure","actual_departure","scheduled_arrival","actual_arrival"):
    if col in dff.columns:
        dff[col] = pd.to_datetime(dff[col], errors="coerce", utc=True)

if "actual_arrival" in dff.columns and "scheduled_arrival" in dff.columns:
    dff["arrival_delay_min"] = (dff["actual_arrival"] - dff["scheduled_arrival"]).dt.total_seconds() / 60.0
else:
    dff["arrival_delay_min"] = np.nan

if "actual_departure" in dff.columns and "scheduled_departure" in dff.columns:
    dff["departure_delay_min"] = (dff["actual_departure"] - dff["scheduled_departure"]).dt.total_seconds() / 60.0
else:
    dff["departure_delay_min"] = np.nan

if "status" in dff.columns:
    valid_mask = (~dff["status"].astype(str).str.lower().eq("cancelled")) & dff["arrival_delay_min"].notna()
else:
    valid_mask = dff["arrival_delay_min"].notna()

if valid_mask.any():
    avg_arrival_delay = round(dff.loc[valid_mask,"arrival_delay_min"].mean(),1)
else:
    avg_arrival_delay = None

if not dff.empty and "destination_iata" in dff.columns:
    per_airport = (
        dff.assign(is_delayed = dff["arrival_delay_min"] > 0)
           .groupby("destination_iata", dropna=True)
           .agg(total_arrivals=("flight_id","count"),
                delayed_arrivals=("is_delayed","sum"),
                avg_delay_min=("arrival_delay_min", lambda s: round(s.dropna().mean(),1)))
           .reset_index()
    )
else:
    per_airport = pd.DataFrame(columns=["destination_iata","total_arrivals","delayed_arrivals","avg_delay_min"])

# ---------------------------------------------------------------------
# Top-level metrics
# ---------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)
col1.metric("Airports", len(df_airports))
col2.metric("Flights", len(df_flights))
unique_aircraft_count = int(df_flights['aircraft_registration'].nunique()) if not df_flights.empty else 0
col3.metric("Unique Aircraft", unique_aircraft_count)
if avg_arrival_delay is None or (isinstance(avg_arrival_delay,float) and math.isnan(avg_arrival_delay)):
    col4.metric("Avg Delay (min)", "N/A")
else:
    col4.metric("Avg Delay (min)", f"{avg_arrival_delay} min")

st.markdown("---")

# ---------------------------------------------------------------------
# Flight search/filter UI
# ---------------------------------------------------------------------
with st.expander("Search / Filter Flights"):
    fs1, fs2, fs3, fs4 = st.columns([2,2,2,1])
    search_number = fs1.text_input("Flight number (partial allowed)", value="")
    airline_filter = fs2.text_input("Airline code (e.g., AI, 6E)", value="")
    status_options = ["Any"] + (sorted(dff['status'].dropna().unique().tolist()) if not dff.empty else [])
    status_sel = fs3.selectbox("Status", status_options)
    date_from = fs4.date_input("Start date", value=None)
    ff = dff.copy()
    if search_number:
        ff = ff[ff['flight_number'].astype(str).str.contains(search_number, case=False, na=False)]
    if airline_filter:
        ff = ff[ff['airline_code'].astype(str).str.contains(airline_filter, case=False, na=False)]
    if status_sel and status_sel != "Any":
        ff = ff[ff['status'] == status_sel]
    if date_from is not None and "scheduled_departure" in ff.columns:
        try:
            date_from_dt = pd.to_datetime(date_from).tz_localize("UTC")
            ff = ff[ff["scheduled_departure"] >= date_from_dt]
        except Exception:
            pass

    display_cols = ["flight_id","flight_number","aircraft_registration","origin_iata","destination_iata","scheduled_departure","actual_departure","scheduled_arrival","actual_arrival","status","airline_code","arrival_delay_min"]
    cols_to_show = [c for c in display_cols if c in ff.columns]
    st.dataframe(ff[cols_to_show].sort_values("scheduled_departure", ascending=False).head(300))

st.markdown("---")

# ---------------------------------------------------------------------
# Airport Details
# ---------------------------------------------------------------------
st.header("Airport Details")
left, right = st.columns([2,3])
with left:
    airport_choices = ["All"] + (sorted(df_airports['iata_code'].dropna().unique().tolist()) if not df_airports.empty else [])
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

# ---------------------------------------------------------------------
# Delay Analysis
# ---------------------------------------------------------------------
st.header("Delay Analysis")
if not per_airport.empty:
    top_delays = per_airport.sort_values("avg_delay_min", ascending=False).head(15)
    fig = px.bar(top_delays, x="destination_iata", y="avg_delay_min", labels={"destination_iata":"Airport","avg_delay_min":"Avg Delay (min)"})
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(per_airport.sort_values("avg_delay_min", ascending=False).head(50))
else:
    st.info("No valid delay data available. Ensure flights have scheduled and actual arrival timestamps.")

st.markdown("---")

# ---------------------------------------------------------------------
# Route leaderboards
# ---------------------------------------------------------------------
st.header("Route Leaderboards")
if not dff.empty:
    route_counts = dff.groupby(['origin_iata','destination_iata']).size().reset_index(name='count').sort_values("count", ascending=False).head(30)
    st.subheader("Busiest routes")
    st.dataframe(route_counts)

    delayed = dff[dff['arrival_delay_min'] > 0].groupby('destination_iata').size().reset_index(name='delayed_count')
    arrivals = dff.groupby('destination_iata').size().reset_index(name='total_arrivals')
    merged = arrivals.merge(delayed, on='destination_iata', how='left').fillna(0)
    merged['pct_delayed'] = (merged['delayed_count'] / merged['total_arrivals'] * 100).round(1)
    st.subheader("Airports by % delayed arrivals")
    st.dataframe(merged.sort_values('pct_delayed', ascending=False).head(20))
else:
    st.info("No flight data available. Use the demo generator or ingestion scripts.")

st.markdown("---")
st.caption("If numbers appear stale, run `streamlit cache clear` and restart the app. On Streamlit Cloud the DB is ephemeral; for persistence use a hosted DB.")
