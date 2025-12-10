"""
Air Tracker: Flight Analytics Dashboard - Fixed version
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import time

from database import FlightDatabase
from data_fetcher import SmartDataFetcher
from config import AIRPORT_CODES

# ==================== PAGE CONFIGURATION ====================
st.set_page_config(
    page_title="Air Tracker",
    page_icon="✈️",
    layout="wide"
)

# ==================== SESSION STATE ====================
if 'db' not in st.session_state:
    st.session_state.db = FlightDatabase()

if 'fetcher' not in st.session_state:
    st.session_state.fetcher = SmartDataFetcher(st.session_state.db)

if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = None

if 'selected_airport' not in st.session_state:
    st.session_state.selected_airport = 'DEL'

# ==================== SIDEBAR ====================
with st.sidebar:
    st.title("✈️ Air Tracker")
    st.markdown("---")
    
    # Refresh button
    if st.button("🔄 Refresh Data", type="primary", use_container_width=True):
        with st.spinner("Fetching latest data..."):
            result = st.session_state.fetcher.fetch_dashboard_data()
            st.session_state.last_refresh = datetime.now()
            
            if result.get('success'):
                st.success(f"✅ Updated {result.get('flights_fetched', 0)} flights")
                st.rerun()
            else:
                st.error("❌ Failed to refresh data")
    
    if st.session_state.last_refresh:
        st.caption(f"Last refresh: {st.session_state.last_refresh.strftime('%H:%M:%S')}")
    
    st.markdown("---")
    
    # Airport selection
    st.subheader("🏢 Select Airport")
    selected = st.selectbox(
        "Choose Airport",
        AIRPORT_CODES,
        index=0
    )
    st.session_state.selected_airport = selected
    
    # Date range
    st.markdown("---")
    st.subheader("📅 Date Range")
    start_date = st.date_input("From", datetime.now() - timedelta(days=3))
    end_date = st.date_input("To", datetime.now())

# ==================== MAIN DASHBOARD ====================
st.title("✈️ Air Tracker: Flight Analytics")

# Tabs
tab1, tab2, tab3 = st.tabs(["📊 Overview", "🔍 Flight Search", "🏢 Airport Details"])

with tab1:
    # Quick stats
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Total airports
        result = st.session_state.db.execute_query("SELECT COUNT(*) FROM airport")
        total_airports = result[0][0] if result else 0
        st.metric("Airports in DB", total_airports)
    
    with col2:
        # Total flights
        result = st.session_state.db.execute_query("SELECT COUNT(*) FROM flights")
        total_flights = result[0][0] if result else 0
        st.metric("Total Flights", total_flights)
    
    with col3:
        # Average delay
        result = st.session_state.db.execute_query(
            "SELECT AVG(avg_delay_min) FROM airport_delays WHERE avg_delay_min > 0"
        )
        avg_delay = result[0][0] if result and result[0][0] else 0
        st.metric("Avg Delay", f"{avg_delay:.1f} min")
    
    st.markdown("---")
    
    # Recent flights
    st.subheader("Recent Flights")
    flights_df = st.session_state.db.execute_query(
        "SELECT flight_number, airline_name, origin_iata, destination_iata, "
        "scheduled_departure, status FROM flights ORDER BY scheduled_departure DESC LIMIT 20",
        return_df=True
    )
    
    if flights_df is not None and not flights_df.empty:
        st.dataframe(flights_df, use_container_width=True)
    else:
        st.info("No flight data. Click 'Refresh Data' to fetch.")
    
    # Flight status chart
    st.markdown("---")
    st.subheader("Flight Status Distribution")
    status_df = st.session_state.db.execute_query(
        "SELECT status, COUNT(*) as count FROM flights GROUP BY status",
        return_df=True
    )
    
    if status_df is not None and not status_df.empty:
        fig = px.pie(status_df, values='count', names='status', title="Flight Status")
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader("Search Flights")
    
    search_col1, search_col2 = st.columns([3, 1])
    
    with search_col1:
        search_query = st.text_input(
            "Search by flight number, route, or airport:",
            placeholder="AI101, DEL-BOM, or JFK"
        )
    
    with search_col2:
        if st.button("Search", type="primary"):
            if search_query:
                with st.spinner("Searching..."):
                    results = st.session_state.fetcher.search_flights(search_query)
                    
                    if results:
                        st.success(f"Found {len(results)} flights")
                        
                        # Display results
                        flights_list = []
                        for flight in results[:15]:
                            flights_list.append({
                                'Flight': flight.get('number', 'N/A'),
                                'Airline': flight.get('airline', {}).get('name', 'N/A'),
                                'From': flight.get('departure', {}).get('airport', {}).get('iata', 'N/A'),
                                'To': flight.get('arrival', {}).get('airport', {}).get('iata', 'N/A'),
                                'Scheduled': flight.get('departure', {}).get('scheduledTime', {}).get('local', 'N/A'),
                                'Status': flight.get('status', 'N/A')
                            })
                        
                        if flights_list:
                            results_df = pd.DataFrame(flights_list)
                            st.dataframe(results_df, use_container_width=True)
                    else:
                        st.info("No flights found")
            else:
                st.warning("Please enter a search query")

with tab3:
    airport = st.session_state.selected_airport
    st.subheader(f"{airport} Airport Details")
    
    if st.button(f"Get Latest Info for {airport}", type="secondary"):
        with st.spinner(f"Fetching details for {airport}..."):
            details = st.session_state.fetcher.fetch_airport_details(airport)
            
            if details.get('basic_info'):
                info = details['basic_info']
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Name:** {info.get('name', 'N/A')}")
                    st.write(f"**City:** {info.get('municipalityName', 'N/A')}")
                    st.write(f"**Country:** {info.get('country', {}).get('name', 'N/A')}")
                
                with col2:
                    st.write(f"**IATA:** {info.get('iata', 'N/A')}")
                    st.write(f"**ICAO:** {info.get('icao', 'N/A')}")
                    st.write(f"**Timezone:** {info.get('timeZone', 'N/A')}")
            
            if details.get('current_flights') and 'data' in details['current_flights']:
                st.markdown("---")
                st.subheader("Current Departures")
                
                flights = details['current_flights']['data'][:10]
                for flight in flights:
                    st.write(f"✈️ {flight.get('number')} to "
                           f"{flight.get('arrival', {}).get('airport', {}).get('iata', 'N/A')} "
                           f"- {flight.get('status')}")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
    <p>✈️ Air Tracker | Flight Analytics Dashboard</p>
    </div>
    """,
    unsafe_allow_html=True
)