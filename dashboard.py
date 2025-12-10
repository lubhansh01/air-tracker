"""
Dashboard for 15 airports
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

from database import FlightDatabase
from data_fetcher import SmartDataFetcher
from config import AIRPORT_CODES, AIRPORT_GROUPS, AIRPORT_NAMES

# ==================== PAGE CONFIG ====================
st.set_page_config(
    page_title="Air Tracker - 15 Airports",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== INITIALIZATION ====================
if 'db' not in st.session_state:
    st.session_state.db = FlightDatabase()

if 'fetcher' not in st.session_state:
    st.session_state.fetcher = SmartDataFetcher(st.session_state.db)

# ==================== SIDEBAR ====================
with st.sidebar:
    st.title("✈️ Air Tracker")
    st.markdown("**15 National & International Airports**")
    
    st.markdown("---")
    
    # Quick stats
    st.subheader("📊 Quick Stats")
    
    # Total airports in DB
    airports = st.session_state.db.execute_query("SELECT COUNT(*) FROM airport")
    st.metric("Airports in DB", airports[0][0] if airports else 0)
    
    # Total flights
    flights = st.session_state.db.execute_query("SELECT COUNT(*) FROM flights")
    st.metric("Total Flights", flights[0][0] if flights else 0)
    
    # Today's flights
    today = st.session_state.db.execute_query(
        "SELECT COUNT(*) FROM flights WHERE DATE(flight_date) = DATE('now')"
    )
    st.metric("Today's Flights", today[0][0] if today else 0)
    
    st.markdown("---")
    
    # Data Fetch Options
    st.subheader("🔄 Data Fetch")
    
    fetch_option = st.radio(
        "Fetch Data For:",
        ["All 15 Airports", "By Region", "Single Airport"]
    )
    
    if fetch_option == "All 15 Airports":
        if st.button("🚀 Fetch All Data", type="primary", use_container_width=True):
            with st.spinner("Fetching data for 15 airports..."):
                result = st.session_state.fetcher.fetch_all_airport_data()
                if result.get('success'):
                    st.success(f"✅ {result['flights_fetched']} flights fetched")
                    st.rerun()
                else:
                    st.error("❌ Fetch failed")
    
    elif fetch_option == "By Region":
        region = st.selectbox("Select Region", list(AIRPORT_GROUPS.keys()))
        if st.button(f"🌍 Fetch {region} Data", type="secondary", use_container_width=True):
            with st.spinner(f"Fetching {region} data..."):
                result = st.session_state.fetcher.fetch_region_data(region)
                if result.get('success'):
                    st.success(f"✅ {result['flights']} flights fetched")
                    st.rerun()
    
    else:  # Single Airport
        airport = st.selectbox("Select Airport", AIRPORT_CODES)
        if st.button(f"✈️ Fetch {airport} Data", type="secondary", use_container_width=True):
            with st.spinner(f"Fetching {airport} data..."):
                # You can implement single airport fetch here
                st.info("Single airport fetch coming soon")
    
    st.markdown("---")
    
    # Airport Selection for Details
    st.subheader("🏢 Airport Details")
    selected_airport = st.selectbox(
        "View Airport",
        AIRPORT_CODES,
        format_func=lambda x: f"{x} - {AIRPORT_NAMES.get(x, x)}",
        index=0
    )
    st.session_state.selected_airport = selected_airport

# ==================== MAIN DASHBOARD ====================
st.title("✈️ Air Tracker: 15 Airports Dashboard")

# Header metrics
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Airports Tracked", len(AIRPORT_CODES))
with col2:
    indian = len(AIRPORT_GROUPS['India'])
    st.metric("Indian Airports", indian)
with col3:
    international = len(AIRPORT_CODES) - indian
    st.metric("International", international)
with col4:
    regions = len(AIRPORT_GROUPS)
    st.metric("Regions", regions)

st.markdown("---")

# Main Tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "🌍 Overview", 
    "✈️ Flight Explorer", 
    "🏢 Airport Details", 
    "📈 Analytics"
])

with tab1:
    # Region-wise stats
    st.subheader("Region-wise Statistics")
    
    region_stats = st.session_state.db.get_airport_stats()
    
    if region_stats is not None and not region_stats.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Bar chart - Airports by region
            fig = px.bar(
                region_stats,
                x='region',
                y='airports',
                title="Airports by Region",
                color='region'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Bar chart - Average delay by region
            fig = px.bar(
                region_stats,
                x='region',
                y='avg_delay',
                title="Average Delay by Region (minutes)",
                color='avg_delay',
                color_continuous_scale='Reds'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Region stats table
        with st.expander("View Region Statistics"):
            st.dataframe(region_stats, use_container_width=True)
    else:
        st.info("No region data available. Fetch data first.")
    
    st.markdown("---")
    
    # Recent flights
    st.subheader("Recent Flight Activity")
    
    recent_flights = st.session_state.db.execute_query(
        "SELECT flight_number, airline_name, origin_iata, destination_iata, "
        "scheduled_departure, status FROM flights "
        "ORDER BY scheduled_departure DESC LIMIT 20",
        return_df=True
    )
    
    if recent_flights is not None and not recent_flights.empty:
        st.dataframe(recent_flights, use_container_width=True)
    else:
        st.info("No flight data. Click 'Fetch All Data' to get started.")

with tab2:
    # Flight Search
    st.subheader("Flight Search")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_query = st.text_input(
            "Search flights:",
            placeholder="Flight number (AI101), Route (DEL-BOM), or Airport code"
        )
    
    with col2:
        if st.button("🔍 Search", type="primary", use_container_width=True):
            if search_query:
                with st.spinner("Searching..."):
                    results = st.session_state.fetcher.search_flights(search_query)
                    
                    if results:
                        st.success(f"Found {len(results)} flights")
                        
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
                st.warning("Please enter search query")
    
    # Popular routes
    st.markdown("---")
    st.subheader("Popular Routes")
    
    popular_routes = st.session_state.db.execute_query(
        "SELECT origin_iata || ' → ' || destination_iata as route, "
        "COUNT(*) as flights FROM flights "
        "GROUP BY origin_iata, destination_iata "
        "ORDER BY flights DESC LIMIT 10",
        return_df=True
    )
    
    if popular_routes is not None and not popular_routes.empty:
        st.dataframe(popular_routes, use_container_width=True)
    else:
        st.info("No route data available")

with tab3:
    airport = st.session_state.selected_airport
    st.header(f"{AIRPORT_NAMES.get(airport, airport)} Airport")
    
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
            st.subheader("Basic Information")
            st.write(f"**Name:** {info['name']}")
            st.write(f"**City:** {info['city']}")
            st.write(f"**Country:** {info['country']}")
            st.write(f"**Region:** {info.get('region', 'N/A')}")
        
        with col2:
            st.subheader("Location & Details")
            st.write(f"**IATA Code:** {info['iata_code']}")
            st.write(f"**ICAO Code:** {info['icao_code']}")
            st.write(f"**Timezone:** {info['timezone']}")
            st.write(f"**Coordinates:** {info['latitude']:.4f}, {info['longitude']:.4f}")
    
    st.markdown("---")
    
    # Airport statistics
    st.subheader("Airport Statistics")
    
    stats_cols = st.columns(4)
    
    with stats_cols[0]:
        # Departures today
        departures = st.session_state.db.execute_query(
            "SELECT COUNT(*) FROM flights WHERE origin_iata = ? AND DATE(flight_date) = DATE('now')",
            (airport,)
        )
        st.metric("Today's Departures", departures[0][0] if departures else 0)
    
    with stats_cols[1]:
        # Arrivals today
        arrivals = st.session_state.db.execute_query(
            "SELECT COUNT(*) FROM flights WHERE destination_iata = ? AND DATE(flight_date) = DATE('now')",
            (airport,)
        )
        st.metric("Today's Arrivals", arrivals[0][0] if arrivals else 0)
    
    with stats_cols[2]:
        # Most common destination
        top_dest = st.session_state.db.execute_query(
            "SELECT destination_iata, COUNT(*) as flights FROM flights "
            "WHERE origin_iata = ? GROUP BY destination_iata "
            "ORDER BY flights DESC LIMIT 1",
            (airport,)
        )
        if top_dest:
            st.metric("Top Destination", top_dest[0][0])
        else:
            st.metric("Top Destination", "N/A")
    
    with stats_cols[3]:
        # Delay info
        delays = st.session_state.db.execute_query(
            "SELECT avg_delay_min FROM airport_delays "
            "WHERE airport_iata = ? ORDER BY delay_date DESC LIMIT 1",
            (airport,)
        )
        if delays and delays[0][0]:
            st.metric("Avg Delay", f"{delays[0][0]:.1f} min")
        else:
            st.metric("Avg Delay", "N/A")
    
    # Recent flights from this airport
    st.markdown("---")
    st.subheader("Recent Departures")
    
    airport_flights = st.session_state.db.execute_query(
        "SELECT flight_number, airline_name, destination_iata, "
        "scheduled_departure, status FROM flights "
        "WHERE origin_iata = ? ORDER BY scheduled_departure DESC LIMIT 10",
        (airport,),
        return_df=True
    )
    
    if airport_flights is not None and not airport_flights.empty:
        st.dataframe(airport_flights, use_container_width=True)
    else:
        st.info(f"No departure data for {airport}")

with tab4:
    st.header("Advanced Analytics")
    
    # Flight status distribution
    st.subheader("Flight Status Overview")
    
    status_data = st.session_state.db.execute_query(
        "SELECT status, COUNT(*) as count FROM flights GROUP BY status",
        return_df=True
    )
    
    if status_data is not None and not status_data.empty:
        fig = px.pie(
            status_data,
            values='count',
            names='status',
            title="Flight Status Distribution"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Top airlines
    st.subheader("Top Airlines")
    
    airlines = st.session_state.db.execute_query(
        "SELECT airline_name, COUNT(*) as flights FROM flights "
        "WHERE airline_name != '' GROUP BY airline_name "
        "ORDER BY flights DESC LIMIT 10",
        return_df=True
    )
    
    if airlines is not None and not airlines.empty:
        fig = px.bar(
            airlines,
            x='flights',
            y='airline_name',
            orientation='h',
            title="Top Airlines by Flight Count"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Delay analysis
    st.subheader("Delay Analysis")
    
    delay_analysis = st.session_state.db.execute_query(
        "SELECT airport_iata, AVG(avg_delay_min) as avg_delay "
        "FROM airport_delays GROUP BY airport_iata "
        "HAVING avg_delay > 0 ORDER BY avg_delay DESC",
        return_df=True
    )
    
    if delay_analysis is not None and not delay_analysis.empty:
        fig = px.bar(
            delay_analysis,
            x='avg_delay',
            y='airport_iata',
            orientation='h',
            title="Average Delay by Airport (minutes)"
        )
        st.plotly_chart(fig, use_container_width=True)

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
    <p>✈️ Air Tracker | 15 National & International Airports | AeroDataBox API</p>
    <p><small>Tracking: 7 Indian airports + 8 International airports</small></p>
    </div>
    """,
    unsafe_allow_html=True
)