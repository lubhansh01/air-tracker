# streamlit_app.py
"""
Air Tracker — Flight Analytics (fixed & integrated)
- tz-safe datetime handling
- date filtering using .date()
- ETL runner passing secrets to subprocess
- homepage, search/filters, airport viewer, delay analysis, leaderboards
"""

import os
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Optional, Dict

import pandas as pd
import streamlit as st
import altair as alt
from datetime import datetime, timedelta, date

# -----------------------
# Config / defaults
# -----------------------
DEFAULT_DB_PATH = "data/airtracker.db"
MAX_LOAD_ROWS = 100_000
ETL_SCRIPT = "scripts/fetch_and_load.py"
DATE_COLS = [
    "scheduled_departure",
    "actual_departure",
    "scheduled_arrival",
    "actual_arrival",
]

# ETL rate-limit seconds (avoid accidental repeated runs)
ETL_COOLDOWN_SECONDS = 300  # 5 minutes

# -----------------------
# Utilities
# -----------------------
def get_aerodatabox_key() -> Optional[str]:
    """Prefer streamlit secrets, then environment variable."""
    try:
        if hasattr(st, "secrets") and "AERODATABOX_API_KEY" in st.secrets:
            return st.secrets["AERODATABOX_API_KEY"]
    except Exception:
        pass
    return os.environ.get("AERODATABOX_API_KEY")


@st.cache_data(ttl=300)
def load_flights_db(db_path: str, limit: int = MAX_LOAD_ROWS) -> pd.DataFrame:
    """Load flights table into a DataFrame and normalize datetimes to tz-naive (UTC) where possible."""
    db = Path(db_path)
    if not db.exists():
        return pd.DataFrame()

    conn = sqlite3.connect(str(db))
    try:
        df = pd.read_sql_query(f"SELECT * FROM flights LIMIT {limit}", conn)
    finally:
        conn.close()

    # Parse and normalize datetime columns to tz-naive (UTC)
    for c in DATE_COLS:
        if c in df.columns:
            try:
                parsed = pd.to_datetime(df[c], errors="coerce")
                # If tz-aware, convert to UTC then drop tzinfo; otherwise keep as-is
                if parsed.dt.tz is not None:
                    try:
                        parsed = parsed.dt.tz_convert("UTC").dt.tz_localize(None)
                    except Exception:
                        # fallback: remove tz info
                        parsed = parsed.dt.tz_localize(None)
                df[c + "_dt"] = parsed
            except Exception:
                df[c + "_dt"] = pd.to_datetime(df[c], errors="coerce")

    # Helper normalized text columns
    if "airline_code" in df.columns:
        df["airline_code_norm"] = df["airline_code"].fillna("").astype(str).str.upper()
    else:
        df["airline_code_norm"] = ""

    for col in ["flight_number", "aircraft_registration", "flight_id"]:
        if col in df.columns:
            df[col + "_norm"] = df[col].fillna("").astype(str)
        else:
            df[col + "_norm"] = ""

    # Build scheduled_dt and actual_dt (prefer arrival, fallback to departure)
    df["scheduled_dt"] = pd.NaT
    df["actual_dt"] = pd.NaT
    if "scheduled_arrival_dt" in df.columns:
        df.loc[df["scheduled_arrival_dt"].notna(), "scheduled_dt"] = df.loc[df["scheduled_arrival_dt"].notna(), "scheduled_arrival_dt"]
    if "scheduled_departure_dt" in df.columns:
        df.loc[df["scheduled_dt"].isna() & df["scheduled_departure_dt"].notna(), "scheduled_dt"] = df.loc[df["scheduled_dt"].isna() & df["scheduled_departure_dt"].notna(), "scheduled_departure_dt"]
    if "actual_arrival_dt" in df.columns:
        df.loc[df["actual_arrival_dt"].notna(), "actual_dt"] = df.loc[df["actual_arrival_dt"].notna(), "actual_arrival_dt"]
    if "actual_departure_dt" in df.columns:
        df.loc[df["actual_dt"].isna() & df["actual_departure_dt"].notna(), "actual_dt"] = df.loc[df["actual_dt"].isna() & df["actual_departure_dt"].notna(), "actual_departure_dt"]

    # Calculate delay_minutes (actual - scheduled) when both exist
    df["delay_minutes"] = pd.NA
    mask = df["scheduled_dt"].notna() & df["actual_dt"].notna()
    if mask.any():
        df.loc[mask, "delay_minutes"] = (df.loc[mask, "actual_dt"] - df.loc[mask, "scheduled_dt"]).dt.total_seconds() / 60.0

    # Normalize origin/destination codes to uppercase strings or None
    for col in ["origin_iata", "destination_iata"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()
            df.loc[df[col] == "", col] = None
            df[col] = df[col].where(df[col].isna(), df[col].str.upper())
        else:
            df[col] = None

    if "status" in df.columns:
        df["status_norm"] = df["status"].fillna("").astype(str)
    else:
        df["status_norm"] = ""

    return df


@st.cache_data(ttl=300)
def load_airports(db_path: str) -> pd.DataFrame:
    """Try to load airport table from DB (supports common table names)."""
    db = Path(db_path)
    if not db.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(str(db))
    try:
        # Try common names
        for name in ("airport", "airports"):
            try:
                df = pd.read_sql_query(f"SELECT * FROM {name}", conn)
                break
            except Exception:
                df = pd.DataFrame()
        if df.empty:
            return df
    finally:
        conn.close()

    # Normalize IATA, lat/lon, timezone, name
    # detect iata column
    iata_candidates = [c for c in df.columns if c.lower() in ("iata", "iata_code")]
    if iata_candidates:
        df["iata"] = df[iata_candidates[0]].astype(str).str.upper()
    else:
        df["iata"] = None

    lat_col = next((c for c in df.columns if c.lower() in ("latitude", "lat")), None)
    lon_col = next((c for c in df.columns if c.lower() in ("longitude", "lon", "lng")), None)
    if lat_col:
        df["lat"] = pd.to_numeric(df[lat_col], errors="coerce")
    else:
        df["lat"] = None
    if lon_col:
        df["lon"] = pd.to_numeric(df[lon_col], errors="coerce")
    else:
        df["lon"] = None

    name_col = next((c for c in df.columns if "name" in c.lower()), None)
    df["name"] = df[name_col] if name_col else None

    tz_col = next((c for c in df.columns if "time" in c.lower()), None)
    df["timezone"] = df[tz_col] if tz_col else None

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


def run_etl_script(timeout_seconds: int = 1800):
    """
    Run ETL script as a subprocess, passing AERODATABOX_API_KEY from st.secrets/env to the subprocess env.
    Returns (returncode, stdout, stderr).
    """
    python_exe = os.environ.get("PYTHON_EXECUTABLE") or "python"
    if not Path(ETL_SCRIPT).exists():
        return 1, "", f"ETL script not found: {ETL_SCRIPT}"

    env = os.environ.copy()
    try:
        if hasattr(st, "secrets") and "AERODATABOX_API_KEY" in st.secrets:
            env["AERODATABOX_API_KEY"] = st.secrets["AERODATABOX_API_KEY"]
    except Exception:
        pass

    try:
        proc = subprocess.run([python_exe, ETL_SCRIPT], capture_output=True, text=True, timeout=timeout_seconds, env=env)
        return proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired:
        return 124, "", "ETL script timed out"
    except Exception as e:
        return 2, "", f"Failed to run ETL script: {e}"


# -----------------------
# Streamlit UI
# -----------------------
st.set_page_config(page_title="Air Tracker — Flight Analytics", layout="wide")
st.title("Air Tracker — Flight Analytics")

# Sidebar
with st.sidebar:
    st.header("Controls & notes")
    st.write("This app is read-only by default and uses a local SQLite DB.")
    st.markdown("DB path (editable):")
    db_path_input = st.text_input("DB path", value=DEFAULT_DB_PATH)
    st.markdown("---")
    st.markdown("To populate the DB:")
    st.markdown("- Use `scripts/import_csv.py` to import a CSV (recommended for demo).")
    st.markdown("- Or run `python scripts/fetch_and_load.py` locally with your AERODATABOX_API_KEY set.")
    st.markdown("- On Streamlit Cloud, add your key in Manage app → Settings → Secrets: `AERODATABOX_API_KEY = \"your_key\"`.")
    st.write("---")
    if st.button("Show environment (debug)"):
        st.write(dict(os.environ))

# Top stats
counts = db_row_counts(db_path_input)
st.header("Homepage Dashboard")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Airports", f"{counts['airports']}")
c2.metric("Flights", f"{counts['flights']}")

# compute average delay and percent delayed
df_all = load_flights_db(db_path_input)
if df_all.empty:
    c3.metric("Average delay (min)", "N/A")
    c4.metric("Percent flights delayed", "0%")
else:
    delays = pd.to_numeric(df_all["delay_minutes"], errors="coerce").dropna()
    if not delays.empty:
        avg_delay = round(float(delays.mean()), 1)
        pct_delayed = round(100.0 * (delays > 0).sum() / max(1, len(df_all)), 1)
        c3.metric("Average delay (min)", f"{avg_delay}")
        c4.metric("Percent flights delayed", f"{pct_delayed}%")
    else:
        c3.metric("Average delay (min)", "N/A")
        c4.metric("Percent flights delayed", "0%")

st.markdown("---")

# SEARCH & FILTER
st.subheader("Search and Filter Flights")
if df_all.empty:
    st.info("No flight data available. Import CSV or run the fetch script.")
else:
    with st.expander("Filters"):
        col1, col2, col3, col4 = st.columns(4)
        origin_options = sorted([x for x in df_all["origin_iata"].dropna().unique()]) if "origin_iata" in df_all.columns else []
        dest_options = sorted([x for x in df_all["destination_iata"].dropna().unique()]) if "destination_iata" in df_all.columns else []
        airline_options = sorted([x for x in df_all["airline_code_norm"].dropna().unique()]) if "airline_code_norm" in df_all.columns else []
        status_options = sorted([x for x in df_all["status_norm"].dropna().unique()]) if "status_norm" in df_all.columns else []

        origin_sel = col1.multiselect("Origin", options=origin_options, default=[])
        dest_sel = col2.multiselect("Destination", options=dest_options, default=[])
        airline_sel = col3.multiselect("Airline", options=airline_options, default=[])
        status_sel = col4.multiselect("Status", options=status_options, default=[])

        # date range filter using scheduled_dt.date()
        date_range = None
        if "scheduled_dt" in df_all.columns:
            min_dt = df_all["scheduled_dt"].min()
            max_dt = df_all["scheduled_dt"].max()
            if pd.notna(min_dt) and pd.notna(max_dt):
                date_range = st.date_input("Scheduled date range", [min_dt.date(), max_dt.date()])
        text_search = st.text_input("Text search (flight number / registration / flight id)")

    # apply filters
    df = df_all.copy()
    if origin_sel:
        df = df[df["origin_iata"].isin(origin_sel)]
    if dest_sel:
        df = df[df["destination_iata"].isin(dest_sel)]
    if airline_sel:
        df = df[df["airline_code_norm"].isin(airline_sel)]
    if status_sel:
        df = df[df["status_norm"].isin(status_sel)]

    # safe date filtering by comparing date component
    if date_range and len(date_range) == 2 and "scheduled_dt" in df.columns:
        start_date = pd.to_datetime(str(date_range[0])).date()
        end_date = pd.to_datetime(str(date_range[1])).date()
        # df["scheduled_dt"].dt.date may produce NaT -> handle with fillna False
        mask = df["scheduled_dt"].dt.date.between(start_date, end_date)
        df = df[mask.fillna(False)]

    if text_search:
        q = text_search.strip().lower()
        mask = (
            df["flight_number_norm"].str.lower().str.contains(q, na=False) |
            df["aircraft_registration_norm"].str.lower().str.contains(q, na=False) |
            df["flight_id_norm"].str.lower().str.contains(q, na=False)
        )
        df = df[mask]

    st.markdown(f"Filtered rows: **{len(df):,}**")

    # paging & display
    page_size = st.number_input("Rows per page", min_value=5, max_value=2000, value=100, step=5)
    page = st.number_input("Page", min_value=1, value=1, step=1)
    start = (page - 1) * page_size
    end = start + page_size
    st.dataframe(df.iloc[start:end].reset_index(drop=True), use_container_width=True)

    # download
    st.download_button("Download filtered CSV", df.to_csv(index=False).encode("utf-8"), file_name="flights_filtered.csv")

st.markdown("---")

# AIRPORT DETAILS VIEWER
st.subheader("Airport Details Viewer")
airports_df = load_airports(db_path_input)
if airports_df.empty:
    st.info("Airport metadata not found in DB.")
else:
    airport_choices = sorted([a for a in airports_df["iata"].dropna().unique()])
    selected_iata = st.selectbox("Select airport (IATA)", options=[""] + airport_choices)
    if selected_iata:
        info = airports_df[airports_df["iata"] == selected_iata].iloc[0].to_dict()
        st.markdown(f"### {info.get('name') or selected_iata} — {selected_iata}")
        st.write("**Timezone:**", info.get("timezone"))
        st.write("**Latitude / Longitude:**", f"{info.get('lat')} / {info.get('lon')}")
        st.write("**City / Country:**", f"{info.get('city') or info.get('country') or 'N/A'}")
        linked = df_all[(df_all["origin_iata"] == selected_iata) | (df_all["destination_iata"] == selected_iata)]
        st.markdown(f"Flights linked to {selected_iata}: {len(linked):,}")
        if not linked.empty:
            st.dataframe(linked.head(200).reset_index(drop=True), use_container_width=True)

st.markdown("---")

# DELAY ANALYSIS
st.subheader("Delay Analysis")
if df_all.empty:
    st.info("No flights to analyze.")
else:
    by_origin = df_all[["origin_iata", "delay_minutes"]].dropna(subset=["origin_iata"])
    if not by_origin.empty:
        stats = by_origin.groupby("origin_iata").agg(
            flights=("delay_minutes", "count"),
            avg_delay=("delay_minutes", lambda x: float(pd.to_numeric(x, errors="coerce").mean())),
            pct_delayed=("delay_minutes", lambda x: 100.0 * (pd.to_numeric(x, errors="coerce") > 0).sum() / max(1, len(x))),
        ).reset_index()
        stats = stats.sort_values("flights", ascending=False)
        top_chart = stats.nlargest(20, "flights")
        chart = alt.Chart(top_chart).mark_bar().encode(
            x=alt.X("avg_delay:Q", title="Avg delay (min)"),
            y=alt.Y("origin_iata:N", sort="-x", title="Origin IATA"),
            tooltip=["origin_iata", "flights", alt.Tooltip("avg_delay:Q", format=".1f"), alt.Tooltip("pct_delayed:Q", format=".1f")]
        ).properties(height=400)
        st.altair_chart(chart, use_container_width=True)

        chart2 = alt.Chart(top_chart).mark_bar(color="#d62728").encode(
            x=alt.X("pct_delayed:Q", title="% flights delayed"),
            y=alt.Y("origin_iata:N", sort="-x", title="Origin IATA"),
            tooltip=["origin_iata", "flights", alt.Tooltip("pct_delayed:Q", format=".1f")]
        ).properties(height=400)
        st.altair_chart(chart2, use_container_width=True)
    else:
        st.info("No delay data available (delay_minutes missing).")

st.markdown("---")

# ROUTE LEADERBOARDS
st.subheader("Route Leaderboards")
if df_all.empty:
    st.info("No flights to compute leaderboards.")
else:
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
        st.info("Route data not available.")

    if "origin_iata" in df_all.columns and "delay_minutes" in df_all.columns:
        agg = (
            df_all.dropna(subset=["origin_iata", "delay_minutes"])
            .groupby("origin_iata")
            .agg(
                avg_delay=("delay_minutes", lambda x: float(pd.to_numeric(x, errors='coerce').mean())),
                flights=("delay_minutes", "count"),
                pct_delayed=("delay_minutes", lambda x: 100.0 * (pd.to_numeric(x, errors='coerce') > 0).sum() / max(1, len(x)))
            )
            .reset_index()
            .sort_values("avg_delay", ascending=False)
        )
        st.markdown("#### Most delayed airports (top 20 by average delay)")
        st.table(agg.head(20).reset_index(drop=True))
    else:
        st.info("No delay metrics available to compute most delayed airports.")

st.markdown("---")

# MANUAL API TEST (one-off)
st.subheader("Manual API test (one call)")
test_iata = st.text_input("IATA code to test", value="DEL", key="manual_api_iata")
if st.button("Run single API test"):
    key = get_aerodatabox_key()
    if not key:
        st.error("No AERODATABOX_API_KEY found in st.secrets or env.")
    else:
        import requests
        host = "aerodatabox.p.rapidapi.com"
        headers = {"x-rapidapi-key": key, "x-rapidapi-host": host}
        url = f"https://{host}/airports/iata/{test_iata.strip().upper()}"
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code != 200:
                st.error(f"HTTP {r.status_code}")
                st.code(r.text[:1000])
            else:
                st.json(r.json())
        except Exception as e:
            st.error(str(e))

st.markdown("---")

# ETL runner with cooldown
st.subheader("Run fetch-and-load ETL (advanced)")
st.write("Runs scripts/fetch_and_load.py using the app Python environment. Be careful with API quotas.")
if "last_etl_run" not in st.session_state:
    st.session_state["last_etl_run"] = 0

st.write(f"Last ETL run: {datetime.fromtimestamp(st.session_state['last_etl_run']).isoformat() if st.session_state['last_etl_run']>0 else 'never'}")

confirm = st.checkbox("I understand this will call external APIs and may consume quota", key="etl_confirm")
if st.button("Run ETL now") and confirm:
    now_ts = time.time()
    elapsed = now_ts - st.session_state["last_etl_run"]
    if elapsed < ETL_COOLDOWN_SECONDS:
        st.error(f"ETL was run {int(elapsed)} seconds ago. Please wait {int(ETL_COOLDOWN_SECONDS - elapsed)} more seconds.")
    else:
        with st.spinner("Running ETL... this may take a while"):
            rc, out, err = run_etl_script()
        st.subheader("ETL stdout")
        st.code(out if out else "(no stdout)")
        st.subheader("ETL stderr")
        st.code(err if err else "(no stderr)")
        if rc == 0:
            st.success("ETL finished successfully. Refresh page to see updated data.")
            st.session_state["last_etl_run"] = time.time()
            # clear cached data so updated DB is reloaded next view
            try:
                load_flights_db.clear()
                load_airports.clear()
            except Exception:
                pass
        else:
            st.error(f"ETL finished with code {rc}")

st.markdown("---")
st.caption("Keep API keys secret. On Streamlit Cloud add AERODATABOX_API_KEY in Manage app → Settings → Secrets.")
