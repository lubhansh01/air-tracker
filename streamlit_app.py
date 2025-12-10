# streamlit_app.py
"""
Air Tracker — Flight Analytics (enhanced)
Features:
- Homepage dashboard with summary statistics
- Search & filter flights
- Airport details viewer (location, timezone, linked flights)
- Delay analysis (averages and % delayed by airport)
- Route leaderboards (busiest routes, most delayed airports)
"""

import os
import sqlite3
from pathlib import Path
from typing import Optional, Dict

import pandas as pd
import streamlit as st
import altair as alt
from datetime import datetime

# CONFIG
DEFAULT_DB_PATH = "data/airtracker.db"
MAX_LOAD_ROWS = 100000  # safe upper bound to load for analysis
DATE_COLS = [
    "scheduled_departure",
    "actual_departure",
    "scheduled_arrival",
    "actual_arrival",
]

# -----------------------
# Utility functions
# -----------------------
def get_db_path() -> str:
    return st.session_state.get("db_path", DEFAULT_DB_PATH)


@st.cache_data(ttl=300)
def load_flights_db(db_path: str, limit: int = MAX_LOAD_ROWS) -> pd.DataFrame:
    """Load flights table into a DataFrame and normalize columns for analysis."""
    db = Path(db_path)
    if not db.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(str(db))
    try:
        df = pd.read_sql_query(f"SELECT * FROM flights LIMIT {limit}", conn)
    finally:
        conn.close()

    # normalized datetime columns (safe)
    for c in DATE_COLS:
        if c in df.columns:
            try:
                df[c + "_dt"] = pd.to_datetime(df[c], errors="coerce")
            except Exception:
                df[c + "_dt"] = pd.NaT
    # lower-case airline/ids for searching convenience
    if "airline_code" in df.columns:
        df["airline_code_norm"] = df["airline_code"].fillna("").astype(str).str.upper()
    else:
        df["airline_code_norm"] = ""
    for col in ["flight_number", "aircraft_registration", "flight_id"]:
        if col in df.columns:
            df[col + "_norm"] = df[col].fillna("").astype(str)
        else:
            df[col + "_norm"] = ""
    # Compute a sensible "scheduled_dt" and "actual_dt" field for delay calculation:
    # prefer arrival times, but fallback to departures if arrival missing.
    df["scheduled_dt"] = None
    df["actual_dt"] = None
    if "scheduled_arrival_dt" in df.columns:
        df.loc[df["scheduled_arrival_dt"].notna(), "scheduled_dt"] = df.loc[df["scheduled_arrival_dt"].notna(), "scheduled_arrival_dt"]
    if "scheduled_departure_dt" in df.columns:
        df.loc[df["scheduled_dt"].isna() & df["scheduled_departure_dt"].notna(), "scheduled_dt"] = df.loc[df["scheduled_dt"].isna() & df["scheduled_departure_dt"].notna(), "scheduled_departure_dt"]
    if "actual_arrival_dt" in df.columns:
        df.loc[df["actual_arrival_dt"].notna(), "actual_dt"] = df.loc[df["actual_arrival_dt"].notna(), "actual_arrival_dt"]
    if "actual_departure_dt" in df.columns:
        df.loc[df["actual_dt"].isna() & df["actual_departure_dt"].notna(), "actual_dt"] = df.loc[df["actual_dt"].isna() & df["actual_departure_dt"].notna(), "actual_departure_dt"]

    # compute delay_minutes (actual - scheduled) in minutes where both present
    df["delay_minutes"] = None
    mask = df["scheduled_dt"].notna() & df["actual_dt"].notna()
    if mask.any():
        df.loc[mask, "delay_minutes"] = (df.loc[mask, "actual_dt"] - df.loc[mask, "scheduled_dt"]).dt.total_seconds() / 60.0

    # Normalize origin/destination to strings
    for col in ["origin_iata", "destination_iata"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.upper().replace({"": None})
        else:
            df[col] = None

    # status normalization
    if "status" in df.columns:
        df["status_norm"] = df["status"].fillna("").astype(str)
    else:
        df["status_norm"] = ""

    return df


@st.cache_data(ttl=300)
def load_airports(db_path: str) -> pd.DataFrame:
    db = Path(db_path)
    if not db.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(str(db))
    try:
        # Some schemas use 'airport' table name
        try:
            df = pd.read_sql_query("SELECT * FROM airport", conn)
        except Exception:
            # try airports table name
            try:
                df = pd.read_sql_query("SELECT * FROM airports", conn)
            except Exception:
                return pd.DataFrame()
    finally:
        conn.close()

    # Normalize IATA/ICAO casing
    for col in ["iata_code", "IATA", "iata"]:
        if col in df.columns:
            df["iata"] = df[col].astype(str).str.upper()
            break
    if "iata" not in df.columns:
        # try other common names
        if "iata_code" in df.columns:
            df["iata"] = df["iata_code"].astype(str).str.upper()
        else:
            df["iata"] = None

    # lat/lon
    for latcol in ("latitude", "lat"):
        if latcol in df.columns:
            df["lat"] = pd.to_numeric(df[latcol], errors="coerce")
            break
    for loncol in ("longitude", "lon", "lng"):
        if loncol in df.columns:
            df["lon"] = pd.to_numeric(df[loncol], errors="coerce")
            break
    # timezone
    tz_candidates = [c for c in df.columns if "time" in c.lower()]
    if tz_candidates:
        df["timezone"] = df[tz_candidates[0]].astype(str)
    else:
        df["timezone"] = None
    # name
    name_cols = [c for c in df.columns if "name" in c.lower()]
    if name_cols:
        df["name"] = df[name_cols[0]].astype(str)
    else:
        df["name"] = None

    return df


def db_row_counts(db_path: str) -> Dict[str, int]:
    db = Path(db_path)
    if not db.exists():
        return {"airports": 0, "flights": 0}
    conn = sqlite3.connect(str(db))
    cur = conn.cursor()
    counts = {"airports": 0, "flights": 0}
    try:
        try:
            cur.execute("SELECT COUNT(*) FROM airport")
            counts["airports"] = cur.fetchone()[0] or 0
        except Exception:
            counts["airports"] = 0
        try:
            cur.execute("SELECT COUNT(*) FROM flights")
            counts["flights"] = cur.fetchone()[0] or 0
        except Exception:
            counts["flights"] = 0
    finally:
        conn.close()
    return counts


# -----------------------
# Streamlit UI
# -----------------------
st.set_page_config(page_title="Air Tracker — Flight Analytics", layout="wide")
st.title("Air Tracker — Flight Analytics")

# Sidebar
with st.sidebar:
    st.header("Quick actions")
    st.write("DB path (editable):")
    db_path_input = st.text_input("DB path", value=get_db_path())
    st.session_state["db_path"] = db_path_input
    st.write("---")
    st.markdown("Use `scripts/import_csv.py` to load CSV data if you have flight CSVs.")
    st.markdown("Or use `scripts/fetch_and_load.py` to fetch via AeroDataBox (requires API key).")

# Top statistics
counts = db_row_counts(db_path_input)
st.header("Homepage Dashboard")
col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
col1.metric("Airports", f"{counts['airports']}")
col2.metric("Flights", f"{counts['flights']}")

# Average delay across airports (global)
df_all = load_flights_db(db_path_input)
if df_all.empty:
    col3.metric("Average delay (min)", "—")
    col4.metric("Percent flights delayed", "—")
else:
    # compute average delay across flights with delay_minutes
    delays = df_all["delay_minutes"].dropna().astype(float)
    if not delays.empty:
        avg_delay = round(float(delays.mean()), 1)
        pct_delayed = round(100.0 * (delays > 0).sum() / len(df_all), 1)
        col3.metric("Average delay (min)", f"{avg_delay}")
        col4.metric("Percent flights delayed", f"{pct_delayed}%")
    else:
        col3.metric("Average delay (min)", "N/A")
        col4.metric("Percent flights delayed", "0%")

st.markdown("---")

# -----------------------
# Search & Filter Flights
# -----------------------
st.subheader("Search and Filter Flights")

if df_all.empty:
    st.info("No flight data available. Import CSV or run the fetch script.")
else:
    # Controls
    st.markdown("Use the filters to narrow flights. Text search matches flight number, registration, flight id.")
    with st.expander("Filters"):
        c1, c2, c3, c4 = st.columns(4)
        # Unique sorted options (dropna)
        origin_options = sorted([x for x in df_all["origin_iata"].dropna().unique()]) if "origin_iata" in df_all.columns else []
        dest_options = sorted([x for x in df_all["destination_iata"].dropna().unique()]) if "destination_iata" in df_all.columns else []
        airline_options = sorted([x for x in df_all["airline_code_norm"].dropna().unique()]) if "airline_code_norm" in df_all.columns else []
        status_options = sorted([x for x in df_all["status_norm"].dropna().unique()]) if "status_norm" in df_all.columns else []

        origin_sel = c1.multiselect("Origin", options=origin_options, default=[])
        dest_sel = c2.multiselect("Destination", options=dest_options, default=[])
        airline_sel = c3.multiselect("Airline", options=airline_options, default=[])
        status_sel = c4.multiselect("Status", options=status_options, default=[])

        # date range (scheduled_dt)
        has_sched = "scheduled_dt" in df_all.columns or "scheduled_departure_dt" in df_all.columns
        if "scheduled_dt" in df_all.columns:
            min_dt = pd.to_datetime(df_all["scheduled_dt"].min())
            max_dt = pd.to_datetime(df_all["scheduled_dt"].max())
        elif "scheduled_departure_dt" in df_all.columns:
            min_dt = df_all["scheduled_departure_dt"].min()
            max_dt = df_all["scheduled_departure_dt"].max()
        else:
            min_dt = max_dt = None

        if pd.notna(min_dt) and pd.notna(max_dt):
            date_range = st.date_input("Scheduled date range", [min_dt.date(), max_dt.date()])
        else:
            date_range = None

        text_search = st.text_input("Text search (flight number / registration / flight id)")

    # Apply filters
    df = df_all.copy()
    if origin_sel:
        df = df[df["origin_iata"].isin(origin_sel)]
    if dest_sel:
        df = df[df["destination_iata"].isin(dest_sel)]
    if airline_sel:
        df = df[df["airline_code_norm"].isin(airline_sel)]
    if status_sel:
        df = df[df["status_norm"].isin(status_sel)]
    if date_range and len(date_range) == 2:
        start_dt = pd.to_datetime(str(date_range[0]))
        end_dt = pd.to_datetime(str(date_range[1])) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        if "scheduled_dt" in df.columns:
            df = df[(df["scheduled_dt"] >= start_dt) & (df["scheduled_dt"] <= end_dt)]
        else:
            st.info("Date filtering not available (no scheduled datetime column).")
    if text_search:
        q = text_search.strip().lower()
        mask = (
            df["flight_number_norm"].str.lower().str.contains(q, na=False) |
            df["aircraft_registration_norm"].str.lower().str.contains(q, na=False) |
            df["flight_id_norm"].str.lower().str.contains(q, na=False)
        )
        df = df[mask]

    st.markdown(f"Filtered rows: **{len(df):,}**")

    # Paging & display
    page_size = st.number_input("Rows per page", min_value=5, max_value=1000, value=100, step=5)
    page = st.number_input("Page", min_value=1, value=1, step=1)
    start = (page - 1) * page_size
    end = start + page_size
    st.dataframe(df.iloc[start:end].reset_index(drop=True), use_container_width=True)

    # Download filtered CSV
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download filtered CSV", csv_bytes, file_name="flights_filtered.csv")

st.markdown("---")

# -----------------------
# Airport Details Viewer
# -----------------------
st.subheader("Airport Details Viewer")

airports_df = load_airports(db_path_input)
if airports_df.empty:
    st.info("Airport metadata not found in DB. If you have airport data, import it into `airport` table.")
else:
    # select airport
    airport_choices = sorted([a for a in airports_df["iata"].dropna().unique()])
    selected_iata = st.selectbox("Select airport (IATA)", options=[""] + airport_choices)
    if selected_iata:
        info = airports_df[airports_df["iata"] == selected_iata].iloc[0].to_dict()
        st.markdown(f"### {info.get('name') or selected_iata} — {selected_iata}")
        st.write("**Timezone:**", info.get("timezone"))
        st.write("**Latitude / Longitude:**", f"{info.get('lat')} / {info.get('lon')}")
        st.write("**Country / City:**", info.get("country") or info.get("city") or "N/A")

        # show linked flights (origin or destination)
        linked = df_all[(df_all["origin_iata"] == selected_iata) | (df_all["destination_iata"] == selected_iata)]
        st.markdown(f"Flights linked to {selected_iata}: {len(linked):,}")
        if not linked.empty:
            st.dataframe(linked.head(200).reset_index(drop=True), use_container_width=True)

st.markdown("---")

# -----------------------
# Delay Analysis
# -----------------------
st.subheader("Delay Analysis")

if df_all.empty:
    st.info("No flights to analyze.")
else:
    # group by airport (origin), compute average delay and percent delayed
    by_origin = (
        df_all[["origin_iata", "delay_minutes"]]
        .dropna(subset=["origin_iata"])
        .copy()
    )
    if not by_origin.empty:
        stats = by_origin.groupby("origin_iata").agg(
            flights=("delay_minutes", "count"),
            avg_delay=("delay_minutes", lambda x: float(pd.to_numeric(x, errors="coerce").mean())),
            pct_delayed=("delay_minutes", lambda x: 100.0 * (pd.to_numeric(x, errors="coerce") > 0).sum() / max(1, len(x))),
        ).reset_index()
        stats = stats.sort_values("flights", ascending=False)
        st.markdown("#### Average delay (minutes) by origin airport (top 20 by flights)")
        top_chart = stats.nlargest(20, "flights")
        chart = alt.Chart(top_chart).mark_bar().encode(
            x=alt.X("avg_delay:Q", title="Avg delay (min)"),
            y=alt.Y("origin_iata:N", sort="-x", title="Origin IATA"),
            tooltip=["origin_iata", "flights", alt.Tooltip("avg_delay:Q", format=".1f"), alt.Tooltip("pct_delayed:Q", format=".1f")]
        ).properties(height=400, width=700)
        st.altair_chart(chart, use_container_width=True)

        st.markdown("#### Delay percentage by origin airport (top 20 by flights)")
        chart2 = alt.Chart(top_chart).mark_bar(color="#d62728").encode(
            x=alt.X("pct_delayed:Q", title="% flights delayed"),
            y=alt.Y("origin_iata:N", sort="-x", title="Origin IATA"),
            tooltip=["origin_iata", "flights", alt.Tooltip("pct_delayed:Q", format=".1f")]
        ).properties(height=400, width=700)
        st.altair_chart(chart2, use_container_width=True)
    else:
        st.info("No delay data available (no delay_minutes).")

st.markdown("---")

# -----------------------
# Route Leaderboards
# -----------------------
st.subheader("Route Leaderboards")

if df_all.empty:
    st.info("No flights to compute leaderboards.")
else:
    # busiest routes (origin->destination), count flights
    if "origin_iata" in df_all.columns and "destination_iata" in df_all.columns:
        route_counts = (
            df_all.dropna(subset=["origin_iata", "destination_iata"])
            .groupby(["origin_iata", "destination_iata"])
            .size()
            .reset_index(name="flights")
            .sort_values("flights", ascending=False)
        )
        st.markdown("#### Busiest routes (top 20)")
        st.table(route_counts.head(20).reset_index(drop=True))
    else:
        st.info("Route data not available (origin/destination missing).")

    # most delayed airports (avg delay)
    if "origin_iata" in df_all.columns and "delay_minutes" in df_all.columns:
        agg = (
            df_all.dropna(subset=["origin_iata", "delay_minutes"])
            .groupby("origin_iata")
            .agg(avg_delay=("delay_minutes", lambda x: float(pd.to_numeric(x, errors='coerce').mean())),
                 flights=("delay_minutes", "count"),
                 pct_delayed=("delay_minutes", lambda x: 100.0 * (pd.to_numeric(x, errors='coerce') > 0).sum() / max(1, len(x))))
            .reset_index()
            .sort_values("avg_delay", ascending=False)
        )
        st.markdown("#### Most delayed airports (by average delay, top 20)")
        st.table(agg.head(20).reset_index(drop=True))
    else:
        st.info("No delay metrics available to compute most delayed airports.")

st.markdown("---")
st.caption("Built for Flight Analytics — use CSV import or ETL to populate DB. Keep API keys safe (st.secrets).")
