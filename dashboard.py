"""
Dashboard using cURL fetcher
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

from database import FlightDatabase
from data_fetcher import SmartDataFetcher

# ==================== SETUP ====================
st.set_page_config(
    page_title="Air Tracker (cURL)",
    page_icon="✈️",
    layout="wide"
)

# Initialize session state
if 'db' not in st.session_state:
    st.session_state.db = FlightDatabase()
    print("✅ Database ready")

if 'fetcher' not in st.session_state:
    st.session_state.fetcher = SmartDataFetcher(st.session_state.db)
    print("✅ cURL Fetcher ready")

# ==================== SIDEBAR ====================
with st.sidebar:
    st.title("✈️ Air Tracker")
    st.markdown("Using cURL API calls")
    
    st.markdown("---")
    
    # Data fetch button
    if st.button("🔄 **Fetch Live Data**", type="primary", use_container_width=True):
        with st.spinner("Fetching via cURL..."):
            result = st.session_state.fetcher.fetch_dashboard_data()
            
            if result.get('success'):
                st.success(f"✅ {result.get('flights_fetched', 0)} flights fetched")
                st.rerun()
            else:
                st.error("❌ Fetch failed")
    
    st.markdown("---")
    
    # Quick stats
    st.subheader("📊 Database Stats")
    
    # Airport count
    airports = st.session_state.db.execute_query("SELECT COUNT(*) FROM airport")
    st.metric("Airports", airports[0][0] if airports else 0)
    
    # Flight count
    flights = st.session_state.db.execute_query("SELECT COUNT(*) FROM flights")
    st.metric("Flights", flights[0][0] if flights else 0)
    
    st.markdown("---")
    
    # Airport selection
    st.subheader("🏢 Select Airport")
    
    # Get airports from DB
    db_airports = st.session_state.db.execute_query(
        "SELECT iata_code FROM airport ORDER BY iata_code",
        return_df=True
    )
    
    if db_airports is not None and not db_airports.empty:
        options = db_airports['iata_code'].tolist()
    else:
        options = ['DEL', 'BOM', 'LHR']
    
    selected = st.selectbox("Airport", options, index=0)
    st.session_state.selected_airport = selected

# ==================== MAIN DASHBOARD ====================
st.title("✈️ Air Tracker - cURL API Edition")

# Quick info
col1, col2, col3 = st.columns(3)
with col1:
    st.info("Using direct cURL commands")
with col2:
    st.info("Real-time AeroDataBox API")
with col3:
    st.info("SQLite storage")

st.markdown("---")

# Tabs
tab1, tab2, tab3 = st.tabs(["📊 Overview", "🔍 Search", "🏢 Airport"])

with tab1:
    # Recent flights
    st.subheader("Recent Flights")
    
    flights_df = st.session_state.db.execute_query(
        "SELECT flight_number, airline_name, origin_iata, destination_iata, "
        "scheduled_departure, status FROM flights "
        "ORDER BY scheduled_departure DESC LIMIT 15",
        return_df=True
    )
    
    if flights_df is not None and not flights_df.empty:
        st.dataframe(flights_df, use_container_width=True)
        
        # Flight status chart
        status_df = st.session_state.db.execute_query(
            "SELECT status, COUNT(*) as count FROM flights GROUP BY status",
            return_df=True
        )
        
        if status_df is not None and not status_df.empty:
            fig = px.pie(status_df, values='count', names='status', 
                        title="Flight Status")
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No flight data. Click 'Fetch Live Data' to get started.")

with tab2:
    st.subheader("Flight Search")
    
    search_query = st.text_input("Enter flight number, route, or airport:")
    
    if st.button("Search", type="primary") and search_query:
        with st.spinner("Searching..."):
            results = st.session_state.fetcher.search_flights(search_query)
            
            if results:
                st.success(f"Found {len(results)} flights")
                
                # Display results
                flights_list = []
                for flight in results[:10]:
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

with tab3:
    airport = st.session_state.selected_airport
    st.header(f"{airport} Airport")
    
    # Get airport info
    airport_info = st.session_state.db.execute_query(
        "SELECT * FROM airport WHERE iata_code = ?",
        (airport,),
        return_df=True
    )
    
    if airport_info is not None and not airport_info.empty:
        info = airport_info.iloc[0]
        
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Name:** {info['name']}")
            st.write(f"**City:** {info['city']}")
            st.write(f"**Country:** {info['country']}")
        with col2:
            st.write(f"**IATA:** {info['iata_code']}")
            st.write(f"**ICAO:** {info['icao_code']}")
            st.write(f"**Timezone:** {info['timezone']}")
    
    # Fetch live button
    if st.button(f"🔄 Get Live Info", type="secondary"):
        with st.spinner(f"Fetching live data for {airport}..."):
            details = st.session_state.fetcher.fetch_airport_details(airport)
            
            if details.get('basic_info'):
                st.success("Live data loaded")
                info = details['basic_info']
                
                st.write(f"**Live Name:** {info.get('name', 'N/A')}")
                st.write(f"**Live City:** {info.get('municipalityName', 'N/A')}")

# Footer
st.markdown("---")
st.markdown("✈️ Using cURL with AeroDataBox API | Real-time flight data")