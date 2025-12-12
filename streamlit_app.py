# streamlit_app.py
import streamlit as st
import pandas as pd
from db import SessionLocal, Airport, Aircraft, Flight, AirportDelay
from sqlalchemy import text
import plotly.express as px

st.set_page_config(page_title="Air Tracker — Flight Analytics", layout="wide")

@st.cache_data(ttl=300)
def load_dataframes():
    session = SessionLocal()
    df_airports = pd.read_sql(session.query(Airport).statement, session.bind)
    df_flights = pd.read_sql(session.query(Flight).statement, session.bind)
    df_aircraft = pd.read_sql(session.query(Aircraft).statement, session.bind)
    df_delays = pd.read_sql(session.query(AirportDelay).statement, session.bind)
    session.close()
    return df_airports, df_flights, df_aircraft, df_delays

df_airports, df_flights, df_aircraft, df_delays = load_dataframes()

st.title("✈️ Air Tracker — Flight Analytics")
st.markdown("Interactive dashboard for airports, flights, and delays.")

# Top-level metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric("Airports", len(df_airports))
col2.metric("Flights", len(df_flights))
col3.metric("Unique Aircraft", df_flights['aircraft_registration'].nunique() if not df_flights.empty else 0)
col4.metric("Avg Delay (min)", int(df_delays['avg_delay_min'].mean()) if not df_delays.empty else "N/A")

st.markdown("---")

# Flight search and filters
with st.expander("Search / Filter Flights"):
    fs1, fs2, fs3, fs4 = st.columns([2,2,2,1])
    search_number = fs1.text_input("Flight number (partial allowed)")
    airline_filter = fs2.text_input("Airline code (e.g., AI, 6E)")
    status_options = ["Any"] + sorted(df_flights['status'].dropna().unique().tolist()) if not df_flights.empty else ["Any"]
    status_sel = fs3.selectbox("Status", status_options)
    date_from = fs4.date_input("Start date", value=None)
    # build filter
    dff = df_flights.copy()
    if search_number:
        dff = dff[dff['flight_number'].str.contains(search_number, case=False, na=False)]
    if airline_filter:
        dff = dff[dff['airline_code'].str.contains(airline_filter, case=False, na=False)]
    if status_sel and status_sel != "Any":
        dff = dff[dff['status'] == status_sel]
    st.dataframe(dff.head(200))

st.markdown("---")

# Airport details viewer
st.header("Airport Details")
left, right = st.columns([2,3])
with left:
    airport_choices = ["All"] + sorted(df_airports['iata_code'].fillna("").unique().tolist())
    sel_airport = st.selectbox("Select airport (IATA)", airport_choices)
    if sel_airport != "All":
        a = df_airports[df_airports['iata_code'] == sel_airport].iloc[0]
        st.write(f"**{a['name']}** — {a['city']}, {a['country']}")
        st.write(f"Timezone: {a.get('timezone', 'N/A')}")
        st.write(f"Coordinates: {a.get('latitude', '')}, {a.get('longitude', '')}")
with right:
    if sel_airport != "All":
        arrivals = df_flights[df_flights['destination_iata'] == sel_airport]
        departures = df_flights[df_flights['origin_iata'] == sel_airport]
        st.subheader("Recent Arrivals")
        st.dataframe(arrivals.sort_values("actual_arrival", ascending=False).head(10))
        st.subheader("Recent Departures")
        st.dataframe(departures.sort_values("actual_departure", ascending=False).head(10))

st.markdown("---")

# Delay analysis
st.header("Delay Analysis")
if not df_delays.empty:
    top_delays = df_delays.sort_values("avg_delay_min", ascending=False).head(10)
    fig = px.bar(top_delays, x="airport_iata", y="avg_delay_min", labels={"avg_delay_min":"Avg Delay (min)", "airport_iata":"Airport"})
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df_delays.sort_values("avg_delay_min", ascending=False).head(20))
else:
    st.info("No airport delay stats found. Populate airport_delays table first.")

st.markdown("---")

# Route leaderboards
st.header("Route Leaderboards")
if not df_flights.empty:
    route_counts = df_flights.groupby(['origin_iata', 'destination_iata']).size().reset_index(name='count').sort_values("count", ascending=False).head(20)
    st.subheader("Busiest routes")
    st.dataframe(route_counts)
    # Most delayed airports
    delayed = df_flights[df_flights['status'] == 'Delayed'].groupby('destination_iata').size().reset_index(name='delayed_count')
    arrivals = df_flights.groupby('destination_iata').size().reset_index(name='total_arrivals')
    merged = arrivals.merge(delayed, on='destination_iata', how='left').fillna(0)
    merged['pct_delayed'] = merged['delayed_count'] / merged['total_arrivals'] * 100
    st.subheader("Airports by % delayed arrivals")
    st.dataframe(merged.sort_values('pct_delayed', ascending=False).head(20))
else:
    st.info("No flight data available. Run data ingestion scripts first.")

st.markdown("---")
st.caption("Data model based on project brief. See queries.sql for sample analytic queries.")
