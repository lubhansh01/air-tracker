"""
Simple Dashboard for 15 Airports
"""

import streamlit as st
import pandas as pd
from datetime import datetime

from database import FlightDatabase
from data_fetcher import SmartDataFetcher

# Page config
st.set_page_config(
    page_title="Air Tracker - 15 Airports",
    page_icon="✈️",
    layout="wide"
)

# Initialize
if 'db' not in st.session_state:
    st.session_state.db = FlightDatabase()

if 'fetcher' not in st.session_state:
    st.session_state.fetcher = SmartDataFetcher(st.session_state.db)

# Sidebar
with st.sidebar:
    st.title("✈️ Air Tracker")
    st.markdown("**15 National & International Airports**")
    
    st.markdown("---")
    
    # Stats
    airports = st.session_state.db.execute_query("SELECT COUNT(*) FROM airport")
    flights = st.session_state.db.execute_query("SELECT COUNT(*) FROM flights")
    
    st.metric("Airports", airports[0][0] if airports else 0)
    st.metric("Flights", flights[0][0] if flights else 0)
    
    st.markdown("---")
    
    # Fetch button
    if st.button("🚀 Fetch Flight Data", type="primary", use_container_width=True):
        with st.spinner("Fetching data for 15 airports..."):
            result = st.session_state.fetcher.fetch_all_data()
            if result.get('success'):
                st.success(f"✅ {result['flights']} flights fetched")
                st.rerun()
            else:
                st.error("❌ Fetch failed")
    
    st.markdown("---")
    
    # Airport selection
    airport_options = ['DEL', 'BOM', 'MAA', 'BLR', 'HYD', 'CCU', 'AMD',
                      'LHR', 'JFK', 'DXB', 'SIN', 'CDG', 'FRA', 'SYD', 'NRT']
    selected = st.selectbox("Select Airport", airport_options)
    st.session_state.selected_airport = selected

# Main content
st.title("✈️ Air Tracker: 15 Airports Dashboard")

# Header
col1, col2, col3 = st.columns(3)
with col1:
    st.info("7 Indian Airports")
with col2:
    st.info("8 International Airports")
with col3:
    st.info("Real-time Data")

st.markdown("---")

# Tabs
tab1, tab2, tab3 = st.tabs(["📊 Overview", "✈️ Flights", "🏢 Airports"])

with tab1:
    # Recent flights
    st.subheader("Recent Flights")
    
    flights_df = st.session_state.db.execute_query(
        "SELECT flight_number, airline_name, origin_iata, destination_iata, "
        "scheduled_departure, status FROM flights ORDER BY scheduled_departure DESC LIMIT 20",
        return_df=True
    )
    
    if flights_df is not None and not flights_df.empty:
        st.dataframe(flights_df, use_container_width=True)
        
        # Flight status
        status_df = st.session_state.db.execute_query(
            "SELECT status, COUNT(*) as count FROM flights GROUP BY status",
            return_df=True
        )
        
        if status_df is not None and not status_df.empty:
            import plotly.express as px
            fig = px.pie(status_df, values='count', names='status', title="Flight Status")
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No flight data. Click 'Fetch Flight Data' to start.")
        
        # Show airport list
        st.subheader("Airports Being Tracked")
        
        indian = ['DEL', 'BOM', 'MAA', 'BLR', 'HYD', 'CCU', 'AMD']
        international = ['LHR', 'JFK', 'DXB', 'SIN', 'CDG', 'FRA', 'SYD', 'NRT']
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Indian Airports (7)**")
            for airport in indian:
                st.write(f"✈️ {airport}")
        
        with col2:
            st.markdown("**International Airports (8)**")
            for airport in international:
                st.write(f"✈️ {airport}")

with tab2:
    # Flight search
    st.subheader("Flight Search")
    
    search = st.text_input("Enter flight number (e.g., AI101):")
    
    if st.button("Search") and search:
        results = st.session_state.fetcher.search_flights(search)
        
        if results:
            st.success(f"Found {len(results)} flights")
            
            flights_list = []
            for flight in results:
                flights_list.append({
                    'Flight': flight.get('number', ''),
                    'Airline': flight.get('airline', {}).get('name', ''),
                    'From': flight.get('departure', {}).get('airport', {}).get('iata', ''),
                    'To': flight.get('arrival', {}).get('airport', {}).get('iata', ''),
                    'Scheduled': flight.get('departure', {}).get('scheduledTime', {}).get('local', ''),
                    'Status': flight.get('status', '')
                })
            
            if flights_list:
                df = pd.DataFrame(flights_list)
                st.dataframe(df, use_container_width=True)
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
            st.write(f"**IATA Code:** {info['iata_code']}")
            st.write(f"**Coordinates:** {info['latitude']:.4f}, {info['longitude']:.4f}")
    
    # Fetch live button
    if st.button(f"🔄 Get Live Data for {airport}"):
        with st.spinner(f"Fetching live data..."):
            details = st.session_state.fetcher.fetch_airport_details(airport)
            
            if details.get('basic_info'):
                info = details['basic_info']
                st.success("Live data loaded")
                st.write(f"**Live Name:** {info.get('name', '')}")
                st.write(f"**Live City:** {info.get('municipalityName', '')}")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
    <p>✈️ Air Tracker | 15 Airports | AeroDataBox API</p>
    </div>
    """,
    unsafe_allow_html=True
)