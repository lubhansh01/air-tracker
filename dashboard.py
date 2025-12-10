"""
Air Tracker: Flight Analytics Dashboard
Complete Streamlit application for flight data visualization
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import sys
import os

# Add project directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import FlightDatabase
from aerodatabox_client import AeroDataBoxClient
from data_fetcher import SmartDataFetcher
import config

# ==================== PAGE CONFIGURATION ====================
st.set_page_config(
    page_title="Air Tracker: Flight Analytics",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== CUSTOM CSS ====================
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 1.5rem;
        font-weight: 700;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #374151;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
        font-weight: 600;
        border-bottom: 2px solid #E5E7EB;
        padding-bottom: 0.5rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 12px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
    .metric-value {
        font-size: 2.2rem;
        font-weight: 700;
        margin: 0.5rem 0;
    }
    .metric-label {
        font-size: 0.9rem;
        opacity: 0.9;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .stDataFrame {
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .success-box {
        background-color: #D1FAE5;
        border-left: 4px solid #10B981;
        padding: 1rem;
        border-radius: 4px;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #DBEAFE;
        border-left: 4px solid #3B82F6;
        padding: 1rem;
        border-radius: 4px;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# ==================== SESSION STATE INITIALIZATION ====================
def init_session_state():
    """Initialize session state variables"""
    if 'db' not in st.session_state:
        st.session_state.db = FlightDatabase()
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = None
    if 'client' not in st.session_state:
        st.session_state.client = AeroDataBoxClient()
    if 'fetcher' not in st.session_state:
        st.session_state.fetcher = SmartDataFetcher(st.session_state.db)
    if 'selected_airport' not in st.session_state:
        st.session_state.selected_airport = 'DEL'
    if 'kpi_data' not in st.session_state:
        st.session_state.kpi_data = {}

init_session_state()

# ==================== HELPER FUNCTIONS ====================
def update_kpis():
    """Update KPI metrics from database"""
    try:
        db = st.session_state.db
        
        # Total airports
        total_airports = db.execute_query("SELECT COUNT(*) FROM airport")[0][0]
        
        # Total flights (last 7 days)
        total_flights = db.execute_query("""
            SELECT COUNT(*) FROM flights 
            WHERE DATE(flight_date) >= DATE('now', '-7 days')
        """)[0][0]
        
        # Average delay
        avg_delay_result = db.execute_query("""
            SELECT AVG(avg_delay_min) FROM airport_delays 
            WHERE avg_delay_min > 0 AND delay_date = DATE('now')
        """)
        avg_delay = avg_delay_result[0][0] if avg_delay_result and avg_delay_result[0][0] else 0
        
        # Active aircraft
        active_aircraft = db.execute_query("""
            SELECT COUNT(DISTINCT aircraft_registration) 
            FROM flights 
            WHERE aircraft_registration IS NOT NULL 
            AND DATE(flight_date) = DATE('now')
        """)[0][0]
        
        # On-time percentage
        on_time_result = db.execute_query("""
            SELECT 
                SUM(CASE WHEN status IN ('Arrived', 'Landed', 'On Time') THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
            FROM flights 
            WHERE DATE(flight_date) = DATE('now')
        """)
        on_time_pct = on_time_result[0][0] if on_time_result and on_time_result[0][0] else 0
        
        # Delayed flights today
        delayed_today = db.execute_query("""
            SELECT COUNT(*) FROM flights 
            WHERE status = 'Delayed' AND DATE(flight_date) = DATE('now')
        """)[0][0]
        
        st.session_state.kpi_data = {
            'total_airports': total_airports,
            'total_flights': total_flights,
            'avg_delay': avg_delay,
            'active_aircraft': active_aircraft,
            'on_time_pct': on_time_pct,
            'delayed_today': delayed_today
        }
        
    except Exception as e:
        st.error(f"Error updating KPIs: {e}")
        st.session_state.kpi_data = {}

def refresh_data():
    """Refresh data from APIs"""
    try:
        fetcher = st.session_state.fetcher
        
        with st.spinner("Fetching latest data from APIs..."):
            result = fetcher.fetch_dashboard_data()
            st.session_state.last_refresh = datetime.now()
            update_kpis()
            
            if result and result.get('success'):
                return result
            else:
                st.error("Failed to refresh data")
                return None
                
    except Exception as e:
        st.error(f"Error refreshing data: {e}")
        return None

def execute_sql_query(query_number):
    """Execute predefined SQL queries"""
    queries = {
        1: """SELECT a.model, COUNT(f.flight_id) as flight_count
               FROM aircraft a
               JOIN flights f ON a.registration = f.aircraft_registration
               WHERE a.model IS NOT NULL
               GROUP BY a.model
               ORDER BY flight_count DESC""",
        
        2: """SELECT f.aircraft_registration as registration, a.model, 
               COUNT(f.flight_id) as flight_count
               FROM flights f
               JOIN aircraft a ON f.aircraft_registration = a.registration
               GROUP BY f.aircraft_registration, a.model
               HAVING flight_count > 5
               ORDER BY flight_count DESC""",
        
        3: """SELECT a.name as airport_name, COUNT(f.flight_id) as outbound_flights
               FROM flights f
               JOIN airport a ON f.origin_iata = a.iata_code
               GROUP BY a.name
               HAVING outbound_flights > 5
               ORDER BY outbound_flights DESC""",
        
        4: """SELECT a.name as airport_name, a.city, 
               COUNT(f.flight_id) as arrival_count
               FROM flights f
               JOIN airport a ON f.destination_iata = a.iata_code
               GROUP BY a.name, a.city
               ORDER BY arrival_count DESC
               LIMIT 3""",
        
        5: """SELECT f.flight_number, 
               f.origin_iata, 
               f.destination_iata,
               CASE 
                   WHEN o.country = d.country THEN 'Domestic'
                   ELSE 'International'
               END as flight_type
               FROM flights f
               JOIN airport o ON f.origin_iata = o.iata_code
               JOIN airport d ON f.destination_iata = d.iata_code
               WHERE f.flight_date = DATE('now')
               LIMIT 20""",
        
        6: """SELECT f.flight_number, f.aircraft_registration, 
               o.name as departure_airport, f.actual_arrival
               FROM flights f
               JOIN airport o ON f.origin_iata = o.iata_code
               WHERE f.destination_iata = 'DEL' 
               AND f.actual_arrival IS NOT NULL
               ORDER BY f.actual_arrival DESC
               LIMIT 5""",
        
        7: """SELECT a.iata_code, a.name, a.city
               FROM airport a
               LEFT JOIN flights f ON a.iata_code = f.destination_iata
               WHERE f.destination_iata IS NULL""",
        
        8: """SELECT airline_name,
               SUM(CASE WHEN status = 'Arrived' THEN 1 ELSE 0 END) as on_time,
               SUM(CASE WHEN status = 'Delayed' THEN 1 ELSE 0 END) as delayed,
               SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled
               FROM flights
               WHERE airline_name IS NOT NULL
               GROUP BY airline_name
               ORDER BY on_time DESC""",
        
        9: """SELECT f.flight_number, a.model, 
               o.name as origin_airport, d.name as destination_airport,
               f.scheduled_departure
               FROM flights f
               JOIN aircraft a ON f.aircraft_registration = a.registration
               JOIN airport o ON f.origin_iata = o.iata_code
               JOIN airport d ON f.destination_iata = d.iata_code
               WHERE f.status = 'Cancelled'
               ORDER BY f.scheduled_departure DESC""",
        
        10: """SELECT f.origin_iata, f.destination_iata,
                COUNT(DISTINCT a.model) as unique_models
                FROM flights f
                JOIN aircraft a ON f.aircraft_registration = a.registration
                WHERE a.model IS NOT NULL
                GROUP BY f.origin_iata, f.destination_iata
                HAVING unique_models > 2
                ORDER BY unique_models DESC""",
        
        11: """SELECT d.iata_code as airport_code,
                a.name as airport_name,
                ROUND((ad.delayed_flights * 100.0 / NULLIF(ad.total_flights, 0)), 2) as delay_percentage
                FROM airport_delays ad
                JOIN airport a ON ad.airport_iata = a.iata_code
                WHERE ad.delay_date = DATE('now')
                AND ad.total_flights > 0
                ORDER BY delay_percentage DESC
                LIMIT 10"""
    }
    
    if query_number in queries:
        return st.session_state.db.execute_query(queries[query_number], return_df=True)
    return None

# ==================== SIDEBAR ====================
with st.sidebar:
    # Logo and Title
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.image("https://cdn-icons-png.flaticon.com/512/824/824100.png", width=80)
    st.markdown("<h2 style='text-align: center;'>✈️ Air Tracker</h2>", unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Data Refresh Section
    st.subheader("🔄 Data Refresh")
    
    if st.button("🔄 Refresh Live Data", type="primary", use_container_width=True):
        result = refresh_data()
        if result:
            st.success(f"✅ Updated {result.get('flights_fetched', 0)} flights in {result.get('time', 0):.1f}s")
            st.rerun()
    
    if st.session_state.last_refresh:
        st.caption(f"Last refresh: {st.session_state.last_refresh.strftime('%Y-%m-%d %H:%M:%S')}")
    
    st.markdown("---")
    
    # Filters
    st.subheader("🔍 Filters")
    
    # Airport selection
    airports_df = st.session_state.db.execute_query(
        "SELECT iata_code, name, city FROM airport ORDER BY city",
        return_df=True
    )
    
    if airports_df is not None and not airports_df.empty:
        airport_options = airports_df['iata_code'].tolist()
        
        selected_airport = st.selectbox(
            "Select Airport",
            options=airport_options,
            format_func=lambda x: f"{x} - {airports_df[airports_df['iata_code'] == x]['city'].iloc[0]}",
            index=0 if 'DEL' in airport_options else 0
        )
        st.session_state.selected_airport = selected_airport
    else:
        selected_airport = st.selectbox(
            "Select Airport",
            options=config.AIRPORT_CODES,
            index=0
        )
        st.session_state.selected_airport = selected_airport
    
    # Date range
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From", datetime.now() - timedelta(days=7))
    with col2:
        end_date = st.date_input("To", datetime.now())
    
    # Status filter
    status_options = ['All', 'On Time', 'Delayed', 'Cancelled', 'Active', 'Scheduled']
    selected_status = st.selectbox("Flight Status", status_options)
    
    st.markdown("---")
    
    # Project Info
    with st.expander("📋 Project Info"):
        st.markdown("""
        **Air Tracker: Flight Analytics**
        
        - Real-time flight data from AeroDataBox API
        - Airport network analysis
        - Delay prediction analytics
        - Route optimization insights
        
        **Skills:**
        - Python & Streamlit
        - SQL Database Management
        - API Integration
        - Data Visualization
        """)

# ==================== MAIN CONTENT ====================
# Header
st.markdown('<h1 class="main-header">✈️ Air Tracker: Flight Analytics Dashboard</h1>', unsafe_allow_html=True)

# Update KPIs on initial load
if not st.session_state.kpi_data:
    update_kpis()

# ==================== DASHBOARD TABS ====================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🏠 Overview", 
    "📊 Flight Explorer", 
    "🏢 Airport Analytics", 
    "⏱️ Delay Intelligence", 
    "🔍 Advanced Queries"
])

# Tab 1: Overview Dashboard
with tab1:
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{st.session_state.kpi_data.get("total_airports", 0)}</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Airports Tracked</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{st.session_state.kpi_data.get("total_flights", 0):,}</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Total Flights (7 days)</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        avg_delay = st.session_state.kpi_data.get("avg_delay", 0)
        delay_color = "#10B981" if avg_delay < 15 else "#F59E0B" if avg_delay < 30 else "#EF4444"
        st.markdown(f'<div class="metric-card" style="background: linear-gradient(135deg, {delay_color} 0%, {delay_color}80 100%);">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{avg_delay:.1f}m</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Avg. Delay Time</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{st.session_state.kpi_data.get("active_aircraft", 0)}</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Active Aircraft Today</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Charts Row
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<h3 class="sub-header">Flight Status Distribution</h3>', unsafe_allow_html=True)
        status_data = st.session_state.db.execute_query('''
            SELECT 
                CASE 
                    WHEN status IN ('Arrived', 'Landed') THEN 'On Time'
                    WHEN status = 'Delayed' THEN 'Delayed'
                    WHEN status = 'Cancelled' THEN 'Cancelled'
                    ELSE 'Other'
                END as status_category,
                COUNT(*) as count
            FROM flights
            WHERE DATE(flight_date) >= DATE('now', '-7 days')
            GROUP BY status_category
        ''', return_df=True)
        
        if status_data is not None and not status_data.empty:
            fig = px.pie(
                status_data, 
                values='count', 
                names='status_category',
                color_discrete_sequence=px.colors.sequential.Blues_r,
                hole=0.4
            )
            fig.update_layout(
                showlegend=True,
                height=400,
                margin=dict(t=0, b=0, l=0, r=0)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No flight status data available")
    
    with col2:
        st.markdown('<h3 class="sub-header">Top Airlines (Flights Today)</h3>', unsafe_allow_html=True)
        airline_data = st.session_state.db.execute_query('''
            SELECT airline_name, COUNT(*) as flight_count
            FROM flights
            WHERE DATE(flight_date) = DATE('now')
            AND airline_name IS NOT NULL
            AND airline_name != ''
            GROUP BY airline_name
            ORDER BY flight_count DESC
            LIMIT 8
        ''', return_df=True)
        
        if airline_data is not None and not airline_data.empty:
            fig = px.bar(
                airline_data,
                x='flight_count',
                y='airline_name',
                orientation='h',
                color='flight_count',
                color_continuous_scale='Viridis',
                text='flight_count'
            )
            fig.update_layout(
                xaxis_title="Number of Flights",
                yaxis_title="",
                height=400,
                showlegend=False,
                margin=dict(t=30, b=0, l=0, r=0)
            )
            fig.update_traces(texttemplate='%{text}', textposition='outside')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No airline data available")
    
    # Recent Activity
    st.markdown('<h3 class="sub-header">Recent Flight Activity</h3>', unsafe_allow_html=True)
    recent_flights = st.session_state.db.execute_query('''
        SELECT 
            flight_number,
            airline_name,
            origin_iata,
            destination_iata,
            scheduled_departure,
            status,
            aircraft_registration
        FROM flights
        ORDER BY scheduled_departure DESC
        LIMIT 15
    ''', return_df=True)
    
    if recent_flights is not None and not recent_flights.empty:
        st.dataframe(recent_flights, use_container_width=True, height=300)
    else:
        st.info("No recent flight data available")

# Tab 2: Flight Explorer
with tab2:
    st.markdown('<h2 class="sub-header">Flight Search & Exploration</h2>', unsafe_allow_html=True)
    
    # Search Section
    col1, col2, col3 = st.columns([3, 2, 2])
    
    with col1:
        search_query = st.text_input(
            "🔍 Search flights by number, airline, or route:",
            placeholder="e.g., AI101, Air India, DEL-BOM"
        )
    
    with col2:
        flight_date = st.date_input("Flight Date", datetime.now())
    
    with col3:
        search_button = st.button("Search Flights", type="primary", use_container_width=True)
    
    if search_button and search_query:
        with st.spinner("Searching flights..."):
            results = st.session_state.fetcher.search_flights(search_query)
            
            if results:
                st.success(f"Found {len(results)} flights")
                
                # Convert to DataFrame for display
                flights_list = []
                for flight in results[:20]:  # Limit to 20 flights
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
                        file_name=f"flight_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            else:
                st.info("No flights found matching your criteria.")
    
    # Recent flights from database
    st.markdown("---")
    st.markdown("### Recent Flights from Database")
    
    db_flights = st.session_state.db.execute_query('''
        SELECT flight_number, airline_name, origin_iata, destination_iata,
               scheduled_departure, status, aircraft_registration
        FROM flights
        ORDER BY scheduled_departure DESC
        LIMIT 50
    ''', return_df=True)
    
    if db_flights is not None and not db_flights.empty:
        st.dataframe(db_flights, use_container_width=True)
    else:
        st.info("No flight data in database. Click 'Refresh Live Data' to fetch data.")

# Tab 3: Airport Analytics
with tab3:
    st.markdown('<h2 class="sub-header">Airport Analytics</h2>', unsafe_allow_html=True)
    
    selected_airport = st.session_state.selected_airport
    
    # Airport details section
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown(f"### {selected_airport} Airport Details")
        
        if st.button(f"🔍 Get Latest Info for {selected_airport}", type="secondary"):
            with st.spinner(f"Fetching details for {selected_airport}..."):
                details = st.session_state.fetcher.fetch_airport_details(selected_airport)
                
                if details.get('basic_info'):
                    info = details['basic_info']
                    st.write(f"**Name:** {info.get('name', 'N/A')}")
                    st.write(f"**City:** {info.get('municipalityName', 'N/A')}")
                    st.write(f"**Country:** {info.get('country', {}).get('name', 'N/A')}")
                    st.write(f"**Timezone:** {info.get('timeZone', 'N/A')}")
                    
                    if info.get('location'):
                        st.write(f"**Coordinates:** {info['location'].get('lat', 'N/A')}, {info['location'].get('lon', 'N/A')}")
                
                if details.get('local_time'):
                    st.write(f"**Local Time:** {details['local_time'].get('local', 'N/A')}")
                
                if details.get('weather'):
                    weather = details['weather']
                    st.write(f"**Weather:** {weather.get('weather', {}).get('description', 'N/A')}")
                    st.write(f"**Temperature:** {weather.get('main', {}).get('temp', 'N/A')}°C")
    
    with col2:
        # Quick stats
        st.markdown("### Quick Stats")
        
        # Get stats from database
        stats_query = '''
        SELECT 
            (SELECT COUNT(*) FROM flights WHERE origin_iata = ? AND DATE(flight_date) = DATE('now')) as departures,
            (SELECT COUNT(*) FROM flights WHERE destination_iata = ? AND DATE(flight_date) = DATE('now')) as arrivals,
            (SELECT COUNT(*) FROM flights WHERE (origin_iata = ? OR destination_iata = ?) AND status = 'Delayed') as delayed
        '''
        
        stats_result = st.session_state.db.execute_query(
            stats_query, 
            (selected_airport, selected_airport, selected_airport, selected_airport)
        )
        
        if stats_result:
            departures, arrivals, delayed = stats_result[0]
            st.metric("Today's Departures", departures or 0)
            st.metric("Today's Arrivals", arrivals or 0)
            st.metric("Delayed Flights", delayed or 0)
    
    st.markdown("---")
    
    # Airport flights
    st.markdown("### Recent Flight Activity")
    
    if st.button(f"✈️ Get Current Flights for {selected_airport}", type="primary"):
        with st.spinner(f"Fetching flights for {selected_airport}..."):
            flights_data = st.session_state.client.get_airport_flights(selected_airport, 'departures')
            
            if flights_data and 'data' in flights_data:
                flights_list = []
                for flight in flights_data['data'][:15]:  # Limit to 15 flights
                    flights_list.append({
                        'Flight': flight.get('number', 'N/A'),
                        'Airline': flight.get('airline', {}).get('name', 'N/A'),
                        'To': flight.get('arrival', {}).get('airport', {}).get('iata', 'N/A'),
                        'Scheduled': flight.get('departure', {}).get('scheduledTime', {}).get('local', 'N/A'),
                        'Status': flight.get('status', 'N/A'),
                        'Aircraft': flight.get('aircraft', {}).get('reg', 'N/A')
                    })
                
                if flights_list:
                    flights_df = pd.DataFrame(flights_list)
                    st.dataframe(flights_df, use_container_width=True)
                else:
                    st.info("No current flights found")
            else:
                st.info("Could not fetch flight data")

# Tab 4: Delay Intelligence
with tab4:
    st.markdown('<h2 class="sub-header">Delay Intelligence</h2>', unsafe_allow_html=True)
    
    # Global delays
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🌍 Get Global Delay Report", use_container_width=True):
            with st.spinner("Fetching global delay statistics..."):
                global_delays = st.session_state.client.get_global_delays()
                
                if global_delays:
                    st.success("Global delay data loaded")
                    
                    col_a, col_b, col_c = st.columns(3)
                    with col_a:
                        st.metric("Total Flights", global_delays.get('totalFlights', 0))
                    with col_b:
                        st.metric("Delayed", global_delays.get('delayedFlights', 0))
                    with col_c:
                        st.metric("Avg Delay", f"{global_delays.get('averageDelayMinutes', 0)}m")
                else:
                    st.warning("Could not fetch global delays")
    
    with col2:
        selected_for_delay = st.selectbox(
            "Select Airport for Delay Analysis",
            options=config.AIRPORT_CODES,
            index=0
        )
        
        if st.button(f"⏱️ Analyze {selected_for_delay} Delays", use_container_width=True):
            with st.spinner(f"Analyzing delays for {selected_for_delay}..."):
                delays = st.session_state.client.get_airport_delays(selected_for_delay)
                
                if delays:
                    st.success(f"Delay analysis for {selected_for_delay}")
                    
                    # Display delay statistics
                    stats = delays.get('statistics', {})
                    flights = stats.get('flights', {})
                    
                    col_a, col_b, col_c = st.columns(3)
                    with col_a:
                        st.metric("Total Flights", flights.get('total', 0))
                    with col_b:
                        st.metric("Delayed", flights.get('delayed', 0))
                    with col_c:
                        st.metric("Canceled", flights.get('canceled', 0))
                    
                    # Create visualization
                    delay_data = {
                        'Status': ['On Time', 'Delayed', 'Canceled'],
                        'Count': [
                            flights.get('total', 0) - flights.get('delayed', 0) - flights.get('canceled', 0),
                            flights.get('delayed', 0),
                            flights.get('canceled', 0)
                        ]
                    }
                    
                    df = pd.DataFrame(delay_data)
                    fig = px.pie(df, values='Count', names='Status', 
                                title=f"Flight Status at {selected_for_delay}")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning(f"No delay data available for {selected_for_delay}")
    
    st.markdown("---")
    
    # Historical delay trends from database
    st.markdown("### Historical Delay Trends")
    
    delay_trends = st.session_state.db.execute_query('''
        SELECT delay_date, airport_iata, delayed_flights, total_flights,
               ROUND((delayed_flights * 100.0 / NULLIF(total_flights, 0)), 2) as delay_percentage
        FROM airport_delays
        WHERE delay_date >= DATE('now', '-7 days')
        ORDER BY delay_date DESC, delay_percentage DESC
    ''', return_df=True)
    
    if delay_trends is not None and not delay_trends.empty:
        # Pivot for chart
        pivot_data = delay_trends.pivot_table(
            index='delay_date',
            columns='airport_iata',
            values='delay_percentage',
            aggfunc='mean'
        ).fillna(0)
        
        fig = px.line(
            pivot_data,
            title="Delay Percentage Trends (Last 7 Days)",
            markers=True
        )
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Delay Percentage (%)",
            hovermode="x unified"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Show data table
        with st.expander("View Raw Data"):
            st.dataframe(delay_trends, use_container_width=True)
    else:
        st.info("No historical delay data available. Refresh data to see trends.")

# Tab 5: Advanced Queries
with tab5:
    st.markdown('<h2 class="sub-header">Advanced Analytics Queries</h2>', unsafe_allow_html=True)
    
    # Query selection
    query_options = {
        "1. Flights by Aircraft Model": 1,
        "2. High-Utilization Aircraft": 2,
        "3. Busiest Airports (Outbound)": 3,
        "4. Top Destination Airports": 4,
        "5. Domestic vs International": 5,
        "6. Recent Arrivals at DEL": 6,
        "7. Airports with No Arrivals": 7,
        "8. Airline Performance": 8,
        "9. Cancelled Flights": 9,
        "10. Diverse Aircraft Routes": 10,
        "11. Airport Delay Percentage": 11
    }
    
    selected_query_name = st.selectbox(
        "Select Analysis Query",
        options=list(query_options.keys())
    )
    
    if st.button("Run Query", type="primary"):
        query_number = query_options[selected_query_name]
        
        with st.spinner("Running analysis..."):
            results = execute_sql_query(query_number)
            
            if results is not None and not results.empty:
                st.success(f"Query returned {len(results)} rows")
                st.dataframe(results, use_container_width=True)
                
                # Add visualizations for certain queries
                if query_number == 1:  # Flights by Aircraft Model
                    if len(results) > 0:
                        fig = px.bar(
                            results.head(10),
                            x='flight_count',
                            y='model',
                            orientation='h',
                            title="Top Aircraft Models by Flight Count"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                
                elif query_number == 3:  # Busiest Airports
                    if len(results) > 0:
                        fig = px.bar(
                            results,
                            x='outbound_flights',
                            y='airport_name',
                            orientation='h',
                            title="Busiest Airports by Outbound Flights"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                
                elif query_number == 5:  # Domestic vs International
                    if len(results) > 0:
                        flight_types = results['flight_type'].value_counts()
                        fig = px.pie(
                            values=flight_types.values,
                            names=flight_types.index,
                            title="Domestic vs International Flights"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                
                elif query_number == 8:  # Airline Performance
                    if len(results) > 0:
                        # Calculate on-time percentage
                        results['on_time_pct'] = (results['on_time'] * 100.0 / 
                                                (results['on_time'] + results['delayed'] + results['cancelled'])).round(2)
                        
                        top_airlines = results.nlargest(10, 'on_time_pct')
                        fig = px.bar(
                            top_airlines,
                            x='on_time_pct',
                            y='airline_name',
                            orientation='h',
                            title="Top Airlines by On-Time Percentage"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                
                elif query_number == 11:  # Airport Delay Percentage
                    if len(results) > 0:
                        fig = px.bar(
                            results,
                            x='delay_percentage',
                            y='airport_name',
                            orientation='h',
                            title="Airports by Delay Percentage"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                
                # Export option
                csv = results.to_csv(index=False)
                st.download_button(
                    label="📥 Download Results (CSV)",
                    data=csv,
                    file_name=f"query_{query_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No results found for this query.")

# ==================== FOOTER ====================
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
    <p>✈️ Air Tracker: Flight Analytics Dashboard | Powered by AeroDataBox API</p>
    <p>Real-time aviation data with intelligent analytics</p>
    </div>
    """,
    unsafe_allow_html=True
)