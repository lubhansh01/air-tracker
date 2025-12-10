# streamlit_app.py
"""
AirTracker — Layout + interactive sections (Dashboard, Flights, Airports, Analytics)
Recreates the sections/options from the screenshots and connects to the DB + ETL.
Paste as streamlit_app.py (overwrite).
"""

import os
import sqlite3
import subprocess
import time
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st
import altair as alt

# ----------------------
# CONFIG
# ----------------------
DEFAULT_DB_PATH = "data/airtracker.db"
ETL_SCRIPT = "scripts/fetch_and_load.py"
ETL_COOLDOWN_SECONDS = 300  # prevent accidental repeated runs

# columns with datetimes to parse
DATE_COLS = [
    "scheduled_departure",
    "actual_departure",
    "scheduled_arrival",
    "actual_arrival",
]

st.set_page_config(page_title="AirTracker — Flight Analytics", layout="wide", initial_sidebar_state="collapsed")

# ----------------------
# Helpers: DB loaders + ETL runner (tz-safe)
# ----------------------
def get_aerodatabox_key() -> str | None:
    try:
        if hasattr(st, "secrets") and "AERODATABOX_API_KEY" in st.secrets:
            return st.secrets["AERODATABOX_API_KEY"]
    except Exception:
        pass
    return os.environ.get("AERODATABOX_API_KEY")


@st.cache_data(ttl=300)
def load_flights_db(db_path: str) -> pd.DataFrame:
    db = Path(db_path)
    if not db.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(str(db))
    try:
        df = pd.read_sql_query("SELECT * FROM flights", conn)
    finally:
        conn.close()

    # parse datetimes safely (tz-aware or tz-naive -> tz-naive)
    for c in DATE_COLS:
        if c in df.columns:
            parsed = pd.to_datetime(df[c], errors="coerce")
            # if parsed has tz, convert to UTC then drop tzinfo
            try:
                if parsed.dt.tz is not None:
                    parsed = parsed.dt.tz_convert("UTC").dt.tz_localize(None)
            except Exception:
                pass
            df[c + "_dt"] = parsed

    # scheduled_dt/actual_dt columns (prefer arrival then depart)
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

    # delay in minutes if both actual and scheduled exist
    df["delay_minutes"] = pd.NA
    mask = df["scheduled_dt"].notna() & df["actual_dt"].notna()
    if mask.any():
        df.loc[mask, "delay_minutes"] = (df.loc[mask, "actual_dt"] - df.loc[mask, "scheduled_dt"]).dt.total_seconds() / 60.0

    # normalized fields for search
    for src, tgt in [("flight_number", "flight_number_norm"), ("flight_id", "flight_id_norm"), ("aircraft_registration", "aircraft_registration_norm")]:
        if src in df.columns:
            df[tgt] = df[src].fillna("").astype(str)
        else:
            df[tgt] = ""

    if "airline_code" in df.columns:
        df["airline_code_norm"] = df["airline_code"].fillna("").astype(str).str.upper()
    else:
        df["airline_code_norm"] = ""

    if "origin_iata" in df.columns:
        df["origin_iata"] = df["origin_iata"].fillna("").astype(str).str.upper().replace({"": None})
    else:
        df["origin_iata"] = None
    if "destination_iata" in df.columns:
        df["destination_iata"] = df["destination_iata"].fillna("").astype(str).str.upper().replace({"": None})
    else:
        df["destination_iata"] = None

    # status normalized
    df["status_norm"] = df["status"].fillna("").astype(str) if "status" in df.columns else ""

    return df


@st.cache_data(ttl=300)
def load_airports(db_path: str) -> pd.DataFrame:
    db = Path(db_path)
    if not db.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(str(db))
    try:
        # try common airport table names
        for tbl in ("airports", "airport", "airport_table"):
            try:
                df = pd.read_sql_query(f"SELECT * FROM {tbl} LIMIT 10000", conn)
                break
            except Exception:
                df = pd.DataFrame()
    finally:
        conn.close()
    if df.empty:
        return df

    # normalize common fields
    if "iata_code" in df.columns:
        df["iata"] = df["iata_code"].fillna("").astype(str).str.upper()
    elif "iata" in df.columns:
        df["iata"] = df["iata"].fillna("").astype(str).str.upper()
    else:
        df["iata"] = None

    # lat/lon
    for cands, out in [(( "latitude", "lat"), "lat"), (("longitude", "lon","lng"), "lon")]:
        col = next((c for c in df.columns if c in cands), None)
        if col:
            df[out] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[out] = None
    # name/timezone
    df["name"] = df[[c for c in df.columns if "name" in c.lower()]].iloc[:, 0] if any("name" in c.lower() for c in df.columns) else None
    tz_col = next((c for c in df.columns if "time" in c.lower()), None)
    df["timezone"] = df[tz_col] if tz_col else None

    return df


def db_counts(db_path: str) -> dict:
    p = Path(db_path)
    if not p.exists():
        return {"airports": 0, "flights": 0}
    conn = sqlite3.connect(str(p))
    cur = conn.cursor()
    counts = {"airports": 0, "flights": 0}
    try:
        try:
            cur.execute("SELECT count(*) FROM airports")
            counts["airports"] = cur.fetchone()[0] or 0
        except Exception:
            try:
                cur.execute("SELECT count(*) FROM airport")
                counts["airports"] = cur.fetchone()[0] or 0
            except Exception:
                counts["airports"] = 0
        try:
            cur.execute("SELECT count(*) FROM flights")
            counts["flights"] = cur.fetchone()[0] or 0
        except Exception:
            counts["flights"] = 0
    finally:
        conn.close()
    return counts


def run_etl_script(timeout_seconds=1800):
    """Run the ETL script as subprocess while passing the AERODATABOX_API_KEY from st.secrets (if present)."""
    python_exe = os.environ.get("PYTHON_EXECUTABLE") or "python"
    if not Path(ETL_SCRIPT).exists():
        return 1, "", f"ETL not found: {ETL_SCRIPT}"
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
        return 124, "", "ETL timed out"
    except Exception as e:
        return 2, "", f"Failed to run ETL: {e}"

# ----------------------
# UI: top nav + hero
# ----------------------
def top_nav(selected_page: str):
    # top nav layout — left logo, center links, right Live Data button
    c1, c2, c3 = st.columns([1, 4, 1])
    with c1:
        st.markdown("<div style='font-weight:bold; font-size:20px;'>✈️ <span style='color:#18A0C7'>Air</span>Tracker</div>", unsafe_allow_html=True)
    with c2:
        cols = st.columns(5)
        names = ["Dashboard", "Flights", "Airports", "Analytics", "Settings"]
        keys = ["dashboard", "flights", "airports", "analytics", "settings"]
        for col, nm, k in zip(cols, names, keys):
            if nm and st.session_state.get("page", selected_page) == k:
                col.markdown(f"<div style='text-align:center; font-weight:600; color:#18A0C7'>{nm}</div>", unsafe_allow_html=True)
            else:
                if col.button(nm):
                    st.session_state["page"] = k
    with c3:
        if st.button("Live Data"):
            st.session_state["page"] = "dashboard"
            st.experimental_rerun()


def hero_section():
    st.markdown("<div style='height:260px; background: linear-gradient(180deg, rgba(7,13,18,1) 0%, rgba(12,16,20,1) 100%); padding:28px 40px; border-radius:8px;'>" +
                "<h1 style='font-size:44px; margin:10px 0 0 0;'><b>AirTracker</b> <span style='color:#18A0C7'>— Flight Analytics</span></h1>" +
                "<p style='color:#9aa6af; margin-top:8px;'>Real-time flight analytics, airport insights, and operational intelligence</p>" +
                "</div>", unsafe_allow_html=True)
    st.markdown("")  # spacing


# ----------------------
# Page: Dashboard
# ----------------------
def page_dashboard(db_path):
    st.header("Dashboard Overview")
    counts = db_counts(db_path)
    df_all = load_flights_db(db_path)

    # KPI tiles
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Total Flights", f"{counts['flights']:,}")
    col2.metric("Active Airports", f"{counts['airports']:,}")
    if not df_all.empty:
        delays = pd.to_numeric(df_all["delay_minutes"], errors="coerce").dropna()
        ontime = 0
        delayed_ct = 0
        if not delays.empty:
            avg_delay = round(float(delays.mean()), 1)
            delayed_ct = (delays > 0).sum()
            ontime_rate = round(100.0 * (delays <= 0).sum() / max(1, len(delays)), 1)
        else:
            avg_delay = "N/A"
            ontime_rate = "N/A"
        col3.metric("On-Time Rate", f"{ontime_rate}%")
        col4.metric("Delayed Flights", f"{delayed_ct}")
        col5.metric("Avg Delay Time", f"{avg_delay if avg_delay != 'N/A' else 'N/A'} min")
        col6.metric("Cancelled", "—")
    else:
        col3.metric("On-Time Rate", "N/A")
        col4.metric("Delayed Flights", "0")
        col5.metric("Avg Delay Time", "N/A")
        col6.metric("Cancelled", "0")

    st.markdown("---")

    # Flight schedule preview and filters
    st.subheader("Flight Schedule")
    df = df_all.copy()
    # Quick filters row
    q1, q2, q3, q4 = st.columns([3, 1, 1, 1])
    search = q1.text_input("Search flights...", key="search_box")
    status_options = sorted(df["status_norm"].unique()) if not df.empty else []
    status = q2.selectbox("All Status", options=["All"] + status_options)
    airlines = sorted(df["airline_code_norm"].unique()) if not df.empty else []
    airline = q3.selectbox("All Airlines", options=["All"] + airlines)
    if q4.button("More Filters"):
        st.session_state["show_filters"] = not st.session_state.get("show_filters", False)

    # advanced filters
    if st.session_state.get("show_filters", False):
        af1, af2, af3 = st.columns(3)
        origin_list = sorted(df["origin_iata"].dropna().unique()) if "origin_iata" in df.columns else []
        dest_list = sorted(df["destination_iata"].dropna().unique()) if "destination_iata" in df.columns else []
        origins = af1.multiselect("Origin", options=origin_list)
        dests = af2.multiselect("Destination", options=dest_list)
        date_input = af3.date_input("Scheduled date range", value=(datetime.utcnow().date(), datetime.utcnow().date()))

        if origins:
            df = df[df["origin_iata"].isin(origins)]
        if dests:
            df = df[df["destination_iata"].isin(dests)]
        if date_input and len(date_input) == 2 and "scheduled_dt" in df.columns:
            sd = pd.to_datetime(str(date_input[0])).date()
            ed = pd.to_datetime(str(date_input[1])).date()
            mask = df["scheduled_dt"].dt.date.between(sd, ed)
            df = df[mask.fillna(False)]

    # apply quick filters
    if status and status != "All":
        df = df[df["status_norm"] == status]
    if airline and airline != "All":
        df = df[df["airline_code_norm"] == airline]
    if search:
        q = search.strip().lower()
        mask = (df["flight_number_norm"].str.lower().str.contains(q, na=False) |
                df["flight_id_norm"].str.lower().str.contains(q, na=False) |
                df["aircraft_registration_norm"].str.lower().str.contains(q, na=False))
        df = df[mask]

    st.write(f"Showing {len(df):,} flights")
    # page through results
    page_size = st.number_input("Rows per page", min_value=5, max_value=500, value=10, step=5)
    page = st.number_input("Page", min_value=1, value=1, step=1)
    start = (page - 1) * page_size
    end = start + page_size
    cols_to_show = [c for c in ["flight_id", "flight_number", "airline_code", "origin_iata", "destination_iata", "scheduled_dt", "actual_dt", "status"] if c in df.columns]
    if cols_to_show:
        st.dataframe(df[cols_to_show].iloc[start:end].reset_index(drop=True), use_container_width=True)
    else:
        st.info("No flight columns available to show.")

    st.markdown("---")


# ----------------------
# Page: Flights (expanded explorer) — reuse filtering UI but show larger table
# ----------------------
def page_flights(db_path):
    st.header("Flights Explorer")
    df_all = load_flights_db(db_path)
    if df_all.empty:
        st.info("No flight data. Use ETL or upload CSV/DB.")
        return

    # big search + filters
    c1, c2, c3, c4 = st.columns([3,1,1,1])
    search = c1.text_input("Search flight number / registration / id")
    status = c2.selectbox("Status", options=["All"] + sorted(df_all["status_norm"].dropna().unique().tolist()))
    airline = c3.selectbox("Airline", options=["All"] + sorted(df_all["airline_code_norm"].dropna().unique().tolist()))
    if c4.button("More filters"):
        st.session_state["show_flights_more"] = not st.session_state.get("show_flights_more", False)

    df = df_all.copy()
    if st.session_state.get("show_flights_more", False):
        af1, af2 = st.columns(2)
        origins = af1.multiselect("Origin", options=sorted(df_all["origin_iata"].dropna().unique()))
        dests = af2.multiselect("Destination", options=sorted(df_all["destination_iata"].dropna().unique()))
        if origins:
            df = df[df["origin_iata"].isin(origins)]
        if dests:
            df = df[df["destination_iata"].isin(dests)]

    if search:
        q = search.strip().lower()
        mask = (df["flight_number_norm"].str.lower().str.contains(q, na=False) |
                df["flight_id_norm"].str.lower().str.contains(q, na=False) |
                df["aircraft_registration_norm"].str.lower().str.contains(q, na=False))
        df = df[mask]
    if status and status != "All":
        df = df[df["status_norm"] == status]
    if airline and airline != "All":
        df = df[df["airline_code_norm"] == airline]

    st.write(f"Flights: {len(df):,}")
    st.dataframe(df.sort_values(by=["scheduled_dt"], ascending=False).reset_index(drop=True), height=600, use_container_width=True)


# ----------------------
# Page: Airports (cards)
# ----------------------
def page_airports(db_path):
    st.header("Monitored Airports")
    airports_df = load_airports(db_path)
    if airports_df.empty:
        st.info("No airport metadata found in DB.")
        return

    # show cards in grid (3 columns)
    cols = st.columns(3)
    card_index = 0
    for _, row in airports_df.iterrows():
        col = cols[card_index % 3]
        iata = row.get("iata") or "N/A"
        name = row.get("name") or ""
        city = row.get("city") if "city" in row.index else ""
        tz = row.get("timezone") or ""
        with col:
            st.markdown(f"### {iata}\n**{name}**\n{city}\n{tz}\n\n---")
        card_index += 1


# ----------------------
# Page: Analytics
# ----------------------
def page_analytics(db_path):
    st.header("Delay Analytics")
    df_all = load_flights_db(db_path)
    if df_all.empty:
        st.info("No flights to analyze.")
        return

    # delay % by airport (origin)
    by_origin = df_all.dropna(subset=["origin_iata", "delay_minutes"])
    if not by_origin.empty:
        stats = by_origin.groupby("origin_iata").agg(
            flights=("delay_minutes", "count"),
            avg_delay=("delay_minutes", lambda x: float(pd.to_numeric(x, errors="coerce").mean())),
            pct_delayed=("delay_minutes", lambda x: 100.0 * (pd.to_numeric(x, errors="coerce") > 0).sum() / max(1, len(x)))
        ).reset_index().sort_values("pct_delayed", ascending=False)
        top = stats.head(10)
        chart = alt.Chart(top).mark_bar().encode(
            x=alt.X("pct_delayed:Q", title="% flights delayed"),
            y=alt.Y("origin_iata:N", sort="-x", title="Airport")
        ).properties(width=600, height=300)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No delay metrics available.")

    # flights by airline (pie)
    by_airline = df_all.groupby("airline_code_norm").size().reset_index(name="count").sort_values("count", ascending=False)
    if not by_airline.empty:
        pie = alt.Chart(by_airline.head(8)).mark_arc(innerRadius=50).encode(
            theta=alt.Theta(field="count", type="quantitative"),
            color=alt.Color(field="airline_code_norm", type="nominal"),
            tooltip=["airline_code_norm", "count"]
        ).properties(width=400, height=300)
        st.altair_chart(pie, use_container_width=False)
    else:
        st.info("No airline aggregates.")


# ----------------------
# Settings & Utilities UI
# ----------------------
def page_settings(db_path):
    st.header("Settings")
    st.markdown("DB path (change if your DB lives elsewhere)")
    new_path = st.text_input("DB path", value=db_path)
    if st.button("Update DB path"):
        st.session_state["db_path"] = new_path
        st.experimental_rerun()

    st.markdown("Streamlit Secrets: add `AERODATABOX_API_KEY` in Manage app → Settings → Secrets")

    st.markdown("---")
    st.subheader("Import CSV into local DB (one-off)")
    csv_path = st.text_input("Local CSV path", value="")
    if st.button("Import CSV to DB") and csv_path:
        # simple CSV->sqlite import (assumes columns compatible with flights table)
        try:
            df = pd.read_csv(csv_path)
            conn = sqlite3.connect(st.session_state.get("db_path", DEFAULT_DB_PATH))
            df.to_sql("flights", conn, if_exists="append", index=False)
            conn.close()
            st.success("Imported CSV rows.")
            # clear caches
            load_flights_db.clear()
        except Exception as e:
            st.error(f"Import failed: {e}")


# ----------------------
# ETL runner UI (bottom)
# ----------------------
def etl_runner_ui(db_path):
    st.header("Run fetch-and-load ETL (advanced)")
    st.write("Runs scripts/fetch_and_load.py (will use AERODATABOX_API_KEY from st.secrets/env).")
    if "last_etl_run" not in st.session_state:
        st.session_state["last_etl_run"] = 0
    st.write(f"Last run: {datetime.fromtimestamp(st.session_state['last_etl_run']).isoformat() if st.session_state['last_etl_run']>0 else 'never'}")
    confirm = st.checkbox("I understand this may consume API quota and run network calls", key="etl_confirm")
    if st.button("Run ETL now") and confirm:
        elapsed = time.time() - st.session_state["last_etl_run"]
        if elapsed < ETL_COOLDOWN_SECONDS:
            st.error(f"ETL ran {int(elapsed)}s ago. Wait {int(ETL_COOLDOWN_SECONDS - elapsed)}s.")
        else:
            with st.spinner("Running ETL..."):
                rc, out, err = run_etl_script()
            st.subheader("ETL stdout")
            st.code(out or "(no stdout)")
            st.subheader("ETL stderr")
            st.code(err or "(no stderr)")
            if rc == 0:
                st.success("ETL completed successfully.")
                st.session_state["last_etl_run"] = time.time()
                # clear caches so updated DB appears
                try:
                    load_flights_db.clear()
                    load_airports.clear()
                except Exception:
                    pass
            else:
                st.error(f"ETL returned code {rc}")


# ----------------------
# Footer
# ----------------------
def footer():
    st.markdown("---")
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        st.markdown("**AirTracker**")
        st.markdown("Comprehensive flight analytics solution.")
    with c2:
        st.markdown("**Quick Links**\n- Dashboard\n- Flights\n- Airports\n- Analytics")
    with c3:
        st.markdown("**Connect**\nGitHub • Twitter • LinkedIn")
    st.markdown("© 2024 AirTracker. Powered by AeroDataBox API.")


# ----------------------
# App entry
# ----------------------
def main():
    # session defaults
    if "page" not in st.session_state:
        st.session_state["page"] = "dashboard"
    if "db_path" not in st.session_state:
        st.session_state["db_path"] = DEFAULT_DB_PATH

    # Top nav + hero
    top_nav(st.session_state["page"])
    hero_section()

    # page routing area (center column)
    page = st.session_state.get("page", "dashboard")
    db_path = st.session_state.get("db_path", DEFAULT_DB_PATH)

    if page == "dashboard":
        page_dashboard(db_path)
    elif page == "flights":
        page_flights(db_path)
    elif page == "airports":
        page_airports(db_path)
    elif page == "analytics":
        page_analytics(db_path)
    elif page == "settings":
        page_settings(db_path)
    else:
        st.info("Page not found — defaulting to dashboard")
        page_dashboard(db_path)

    # ETL runner and footer at bottom
    etl_runner_ui(db_path)
    footer()


if __name__ == "__main__":
    main()
