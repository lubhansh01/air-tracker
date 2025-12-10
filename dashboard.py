"""
Air Tracker: Flight Analytics Dashboard - FIXED DISPLAY
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
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== SESSION STATE ====================
if 'db' not in st.session_state:
    st.session_state.db = FlightDatabase()
    print("✅ Database initialized")

if 'fetcher' not in st.session_state:
    st.session_state.fetcher = SmartDataFetcher(st.session_state.db)
    print("✅ Fetcher initialized")

if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = None

if 'selected_airport' not in st.session_state:
    st.session_state.selected_airport = 'DEL'

# ==================== HELPER FUNCTIONS ====================
def check_database_status():
    """Check what data is available in database"""
    status = {}
    
    # Check airport count
    result = st.session_state.db.execute_query("SELECT COUNT(*) FROM airport")
    status['airports'] = result[0][0] if result else 0
    
    # Check flight count
    result = st.session_state.db.execute_query("SELECT COUNT(*) FROM flights")
    status['flights'] = result[0][0] if result else 0
    
    # Check delay count
    result = st.session_state.db.execute_query("SELECT COUNT(*) FROM airport_delays")
    status['delays'] = result[0][0] if result else 0
    
    return status

def get_sample_data():
    """Get sample data for display"""
    data = {}
    
    # Get sample airports
    airports = st.session_state.db.execute_query(
        "SELECT iata_code, name, city, country FROM airport LIMIT 10",
        return_df=True
    )
    data['airports'] = airports
    
    # Get sample flights
    flights = st.session_state.db.execute_query(
        "SELECT flight_number, airline_name, origin_iata, destination_iata, "
        "scheduled_departure, status FROM flights ORDER BY scheduled_departure DESC LIMIT 20",
        return_df=True
    )
    data['flights'] = flights
    
    # Get delay stats
    delays = st.session_state.db.execute_query(
        "SELECT airport_iata, delay_date, total_flights, delayed_flights, "
        "avg_delay_min FROM airport_delays ORDER BY delay_date DESC LIMIT 10",
        return_df=True
    )
    data['delays'] = delays
    
    return data

# ==================== SIDEBAR ====================
with st.sidebar:
    st.title("✈️ Air Tracker")
    st.markdown("Flight Analytics Dashboard")
    
    st.markdown("---")
    
    # Database Status
    st.subheader("📊 Database Status")
    status = check_database_status()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Airports", status['airports'])
    with col2:
        st.metric("Flights", status['flights'])
    with col3:
        st.metric("Delays", status['delays'])
    
    if status['flights'] == 0:
        st.warning("No flight data found. Refresh to fetch data.")
    
    st.markdown("---")
    
    # Refresh Section
    st.subheader("🔄 Data Refresh")
    
    if st.button("🔄 **Fetch New Data**", type="primary", use_container_width=True):
        with st.spinner("Fetching latest flight data..."):
            # Show progress
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for i in range(5):
                progress = (i + 1) * 20
                progress_bar.progress(progress)
                messages = [
                    "Connecting to aviation APIs...",
                    "Fetching airport information...",
                    "Retrieving flight schedules...",
                    "Getting delay statistics...",
                    "Storing data in database..."
                ]
                status_text.text(messages[i])
                time.sleep(0.5)
            
            # Actual fetch
            result = st.session_state.fetcher.fetch_dashboard_data()
            st.session_state.last_refresh = datetime.now()
            progress_bar.progress(100)
            
            if result.get('success'):
                status_text.text("✅ Data refresh complete!")
                st.success(f"✅ Fetched {result.get('flights_fetched', 0)} flights")
                st.rerun()
            else:
                status_text.text("❌ Refresh failed")
                st.error("Failed to fetch data. Check console for errors.")
    
    if st.session_state.last_refresh:
        st.caption(f"Last refresh: {st.session_state.last_refresh.strftime('%Y-%m-%d %H:%M:%S')}")
    
    st.markdown("---")
    
    # Airport Selection
    st.subheader("🏢 Airport Selection")
    
    # Get airports from database first, fallback to config
    db_airports = st.session_state.db.execute_query(
        "SELECT iata_code FROM airport ORDER BY iata_code",
        return_df=True
    )
    
    if db_airports is not None and not db_airports.empty:
        airport_options = db_airports['iata_code'].tolist()
    else:
        airport_options = AIRPORT_CODES
    
    selected = st.selectbox(
        "Choose Airport",
        airport_options,
        index=0 if 'DEL' in airport_options else 0
    )
    st.session_state.selected_airport = selected
    
    st.markdown("---")
    
    # Quick Actions
    st.subheader("⚡ Quick Actions")
    
    if st.button("📋 View Raw Database", use_container_width=True):
        st.session_state.show_raw_db = not st.session_state.get('show_raw_db', False)
        st.rerun()
    
    if st.button("🔄 Clear & Reset", type="secondary", use_container_width=True):
        # Clear all tables
        tables = ['flights', 'airport_delays', 'airport', 'aircraft']
        for table in tables:
            st.session_state.db.execute_query(f"DELETE FROM {table}")
        st.session_state.db.execute_query("VACUUM")
        st.success("Database cleared!")
        st.rerun()

# ==================== MAIN DASHBOARD ====================
st.title("✈️ Air Tracker: Flight Analytics Dashboard")

# Show raw database if requested
if st.session_state.get('show_raw_db', False):
    st.subheader("📋 Raw Database Contents")
    
    tabs = st.tabs(["Airports", "Flights", "Delays"])
    
    with tabs[0]:
        airports = st.session_state.db.execute_query(
            "SELECT * FROM airport",
            return_df=True
        )
        if airports is not None and not airports.empty:
            st.dataframe(airports, use_container_width=True)
        else:
            st.info("No airport data")
    
    with tabs[1]:
        flights = st.session_state.db.execute_query(
            "SELECT * FROM flights LIMIT 50",
            return_df=True
        )
        if flights is not None and not flights.empty:
            st.dataframe(flights, use_container_width=True)
        else:
            st.info("No flight data")
    
    with tabs[2]:
        delays = st.session_state.db.execute_query(
            "SELECT * FROM airport_delays",
            return_df=True
        )
        if delays is not None and not delays.empty:
            st.dataframe(delays, use_container_width=True)
        else:
            st.info("No delay data")
    
    if st.button("Close Raw View"):
        st.session_state.show_raw_db = False
        st.rerun()
    
    st.markdown("---")

# Main Tabs
tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🔍 Flight Search", "🏢 Airport Details", "📈 Analytics"])

with tab1:
    # Overview Dashboard
    st.header("Dashboard Overview")
    
    # Status row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # Recent flights count
        recent = st.session_state.db.execute_query(
            "SELECT COUNT(*) FROM flights WHERE DATE(flight_date) = DATE('now')"
        )
        count = recent[0][0] if recent else 0
        st.metric("Today's Flights", count)
    
    with col2:
        # On-time percentage
        on_time = st.session_state.db.execute_query(
            "SELECT COUNT(*) FROM flights WHERE status IN ('Arrived', 'Landed', 'On Time')"
        )
        total = st.session_state.db.execute_query("SELECT COUNT(*) FROM flights")
        
        if total and total[0][0] > 0:
            percentage = (on_time[0][0] / total[0][0]) * 100 if on_time else 0
            st.metric("On-Time %", f"{percentage:.1f}%")
        else:
            st.metric("On-Time %", "N/A")
    
    with col3:
        # Average delay
        avg_delay = st.session_state.db.execute_query(
            "SELECT AVG(avg_delay_min) FROM airport_delays WHERE avg_delay_min > 0"
        )
        delay = avg_delay[0][0] if avg_delay and avg_delay[0][0] else 0
        st.metric("Avg Delay", f"{delay:.1f} min")
    
    with col4:
        # Unique airlines
        airlines = st.session_state.db.execute_query(
            "SELECT COUNT(DISTINCT airline_name) FROM flights WHERE airline_name != ''"
        )
        count = airlines[0][0] if airlines else 0
        st.metric("Airlines", count)
    
    st.markdown("---")
    
    # Recent Flights Table
    st.subheader("Recent Flight Activity")
    
    flights_df = st.session_state.db.execute_query(
        "SELECT flight_number, airline_name, origin_iata, destination_iata, "
        "scheduled_departure, status FROM flights "
        "ORDER BY scheduled_departure DESC LIMIT 20",
        return_df=True
    )
    
    if flights_df is not None and not flights_df.empty:
        # Style the dataframe
        def color_status(val):
            if val in ['Arrived', 'Landed', 'On Time']:
                return 'color: green'
            elif val == 'Delayed':
                return 'color: orange'
            elif val == 'Cancelled':
                return 'color: red'
            else:
                return ''
        
        styled_df = flights_df.style.applymap(color_status, subset=['status'])
        st.dataframe(styled_df, use_container_width=True, height=400)
        
        # Show some stats
        col1, col2 = st.columns(2)
        with col1:
            st.caption(f"Showing {len(flights_df)} most recent flights")
        with col2:
            if st.button("Load More Flights"):
                st.session_state.flight_limit = st.session_state.get('flight_limit', 20) + 20
                st.rerun()
    else:
        st.info("No flight data available. Click 'Fetch New Data' to get started.")
        
        # Show sample of what we expect
        with st.expander("What data will appear?"):
            st.write("After fetching data, you'll see:")
            st.write("- ✈️ Flight numbers and airlines")
            st.write("- 🏢 Origin and destination airports")
            st.write("- 🕐 Scheduled departure times")
            st.write("- 📊 Flight status (On Time, Delayed, etc.)")
    
    st.markdown("---")
    
    # Flight Status Chart
    st.subheader("Flight Status Distribution")
    
    status_df = st.session_state.db.execute_query(
        "SELECT status, COUNT(*) as count FROM flights GROUP BY status",
        return_df=True
    )
    
    if status_df is not None and not status_df.empty:
        fig = px.pie(
            status_df, 
            values='count', 
            names='status',
            title="Flight Status Distribution",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No status data available")

with tab2:
    st.header("Flight Search")
    
    search_col1, search_col2 = st.columns([3, 1])
    
    with search_col1:
        search_query = st.text_input(
            "Search flights:",
            placeholder="Enter flight number (AI101), route (DEL-BOM), or airport code"
        )
    
    with search_col2:
        search_type = st.selectbox(
            "Search Type",
            ["Auto Detect", "Flight Number", "Route", "Airport"],
            index=0
        )
    
    if st.button("🔍 Search Flights", type="primary"):
        if search_query:
            with st.spinner("Searching..."):
                results = st.session_state.fetcher.search_flights(search_query)
                
                if results:
                    st.success(f"Found {len(results)} flights")
                    
                    # Display in a nice table
                    flights_list = []
                    for flight in results[:20]:  # Limit to 20
                        flights_list.append({
                            'Flight': flight.get('number', 'N/A'),
                            'Airline': flight.get('airline', {}).get('name', 'N/A'),
                            'From': flight.get('departure', {}).get('airport', {}).get('iata', 'N/A'),
                            'To': flight.get('arrival', {}).get('airport', {}).get('iata', 'N/A'),
                            'Scheduled': flight.get('departure', {}).get('scheduledTime', {}).get('local', 'N/A'),
                            'Status': flight.get('status', 'N/A'),
                            'Aircraft': flight.get('aircraft', {}).get('reg', 'N/A')
                        })
                    
                    if flights_list:
                        results_df = pd.DataFrame(flights_list)
                        st.dataframe(results_df, use_container_width=True)
                        
                        # Export option
                        csv = results_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Results (CSV)",
                            data=csv,
                            file_name=f"flight_search_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime="text/csv"
                        )
                else:
                    st.info("No flights found. Try a different search.")
        else:
            st.warning("Please enter a search query")
    
    # Quick search examples
    st.markdown("---")
    st.subheader("Quick Search Examples")
    
    examples = st.columns(3)
    with examples[0]:
        if st.button("DEL Departures", use_container_width=True):
            st.session_state.search_query = "DEL"
            st.rerun()
    with examples[1]:
        if st.button("DEL-BOM Route", use_container_width=True):
            st.session_state.search_query = "DEL-BOM"
            st.rerun()
    with examples[2]:
        if st.button("Air India Flights", use_container_width=True):
            st.session_state.search_query = "Air India"
            st.rerun()
    
    if st.session_state.get('search_query'):
        search_query = st.session_state.search_query
        del st.session_state.search_query
        st.experimental_set_query_params(search=search_query)

with tab3:
    airport = st.session_state.selected_airport
    st.header(f"{airport} Airport Details")
    
    # Get airport info from database
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
            st.write(f"**Continent:** {info['continent']}")
        
        with col2:
            st.subheader("Location & Time")
            st.write(f"**IATA Code:** {info['iata_code']}")
            st.write(f"**ICAO Code:** {info['icao_code']}")
            st.write(f"**Timezone:** {info['timezone']}")
            st.write(f"**Coordinates:** {info['latitude']:.4f}, {info['longitude']:.4f}")
    
    else:
        st.info(f"No information found for {airport} in database.")
        
        # Try to fetch live data
        if st.button(f"🔄 Get Live Data for {airport}", type="primary"):
            with st.spinner(f"Fetching live data for {airport}..."):
                details = st.session_state.fetcher.fetch_airport_details(airport)
                
                if details.get('basic_info'):
                    info = details['basic_info']
                    st.success(f"Live data loaded for {airport}")
                    
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
                    st.subheader("Live Departures")
                    
                    flights = details['current_flights']['data'][:10]
                    for flight in flights:
                        with st.container():
                            cols = st.columns([2, 2, 2, 1])
                            with cols[0]:
                                st.write(f"**{flight.get('number')}**")
                            with cols[1]:
                                st.write(f"to {flight.get('arrival', {}).get('airport', {}).get('iata', 'N/A')}")
                            with cols[2]:
                                st.write(flight.get('airline', {}).get('name', 'N/A'))
                            with cols[3]:
                                status = flight.get('status', 'N/A')
                                if status == 'Delayed':
                                    st.error(status)
                                elif status in ['Arrived', 'Landed']:
                                    st.success(status)
                                else:
                                    st.info(status)
                            st.divider()
    
    # Airport statistics from database
    st.markdown("---")
    st.subheader("Airport Statistics")
    
    stats_col1, stats_col2, stats_col3 = st.columns(3)
    
    with stats_col1:
        # Departures today
        departures = st.session_state.db.execute_query(
            "SELECT COUNT(*) FROM flights WHERE origin_iata = ? AND DATE(flight_date) = DATE('now')",
            (airport,)
        )
        count = departures[0][0] if departures else 0
        st.metric("Today's Departures", count)
    
    with stats_col2:
        # Arrivals today
        arrivals = st.session_state.db.execute_query(
            "SELECT COUNT(*) FROM flights WHERE destination_iata = ? AND DATE(flight_date) = DATE('now')",
            (airport,)
        )
        count = arrivals[0][0] if arrivals else 0
        st.metric("Today's Arrivals", count)
    
    with stats_col3:
        # Delay stats
        delays = st.session_state.db.execute_query(
            "SELECT avg_delay_min FROM airport_delays WHERE airport_iata = ? ORDER BY delay_date DESC LIMIT 1",
            (airport,)
        )
        if delays and delays[0][0]:
            st.metric("Avg Delay", f"{delays[0][0]:.1f} min")
        else:
            st.metric("Avg Delay", "N/A")

with tab4:
    st.header("Analytics & Insights")
    
    # Data availability check
    status = check_database_status()
    
    if status['flights'] == 0:
        st.warning("No data available for analytics. Fetch data first.")
        st.info("Click 'Fetch New Data' in the sidebar to get started.")
    else:
        # Analytics tabs
        a_tab1, a_tab2, a_tab3 = st.tabs(["📈 Trends", "🏆 Rankings", "🔍 Insights"])
        
        with a_tab1:
            # Flight trends over time
            st.subheader("Flight Trends")
            
            trend_data = st.session_state.db.execute_query(
                "SELECT DATE(flight_date) as date, COUNT(*) as flights "
                "FROM flights GROUP BY DATE(flight_date) ORDER BY date DESC LIMIT 30",
                return_df=True
            )
            
            if trend_data is not None and not trend_data.empty:
                fig = px.line(
                    trend_data,
                    x='date',
                    y='flights',
                    title="Daily Flight Count (Last 30 Days)",
                    markers=True
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Not enough data for trend analysis")
        
        with a_tab2:
            # Top rankings
            st.subheader("Top Airlines")
            
            top_airlines = st.session_state.db.execute_query(
                "SELECT airline_name, COUNT(*) as flights "
                "FROM flights WHERE airline_name != '' "
                "GROUP BY airline_name ORDER BY flights DESC LIMIT 10",
                return_df=True
            )
            
            if top_airlines is not None and not top_airlines.empty:
                fig = px.bar(
                    top_airlines,
                    x='flights',
                    y='airline_name',
                    orientation='h',
                    title="Top Airlines by Flight Count"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            st.subheader("Busiest Routes")
            
            top_routes = st.session_state.db.execute_query(
                "SELECT origin_iata || '-' || destination_iata as route, COUNT(*) as flights "
                "FROM flights GROUP BY origin_iata, destination_iata "
                "ORDER BY flights DESC LIMIT 10",
                return_df=True
            )
            
            if top_routes is not None and not top_routes.empty:
                st.dataframe(top_routes, use_container_width=True)
        
        with a_tab3:
            # Delay insights
            st.subheader("Delay Analysis")
            
            delay_stats = st.session_state.db.execute_query(
                "SELECT airport_iata, AVG(avg_delay_min) as avg_delay, "
                "SUM(delayed_flights) as total_delayed "
                "FROM airport_delays GROUP BY airport_iata "
                "HAVING total_delayed > 0 ORDER BY avg_delay DESC",
                return_df=True
            )
            
            if delay_stats is not None and not delay_stats.empty:
                fig = px.bar(
                    delay_stats,
                    x='avg_delay',
                    y='airport_iata',
                    orientation='h',
                    title="Average Delay by Airport"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No delay data available")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
    <p>✈️ Air Tracker | Flight Analytics Dashboard | Data from AeroDataBox API</p>
    <p><small>If data is not showing, check console for errors and ensure API key is valid</small></p>
    </div>
    """,
    unsafe_allow_html=True
)

# Debug info in expander
with st.expander("🛠️ Debug Information"):
    st.write("**Session State:**")
    st.json({
        'selected_airport': st.session_state.selected_airport,
        'last_refresh': str(st.session_state.last_refresh),
        'database_status': status
    })
    
    st.write("**Config Settings:**")
    st.json({
        'airport_codes': AIRPORT_CODES,
        'api_configured': bool(st.session_state.fetcher.client.stats['total_requests'] > 0)
    })