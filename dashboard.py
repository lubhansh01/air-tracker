"""
Air Tracker: Flight Analytics Dashboard
Main Streamlit application with optimized data fetching
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
from data_orchestrator import DataOrchestrator
from swagger_clients import AeroDataBoxClient
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
    .refresh-btn {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        padding: 0.75rem 1.5rem;
        border-radius: 8px;
        font-weight: 600;
        cursor: pointer;
        transition: transform 0.2s;
    }
    .refresh-btn:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 12px rgba(0,0,0,0.15);
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
    if 'orchestrator' not in st.session_state:
        st.session_state.orchestrator = DataOrchestrator(st.session_state.db)
    if 'client' not in st.session_state:
        st.session_state.client = AeroDataBoxClient()
    if 'auto_refresh' not in st.session_state:
        st.session_state.auto_refresh = False
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

def refresh_data(mode="quick"):
    """Refresh data with specified mode"""
    try:
        orchestrator = st.session_state.orchestrator
        
        if mode == "quick":
            result = orchestrator.quick_refresh()
        elif mode == "full":
            result = orchestrator.fetch_all_data_strategically()
        else:  # smart
            last_refresh = st.session_state.get('last_refresh_time')
            current_time = time.time()
            
            if last_refresh and (current_time - last_refresh) < 1800:  # 30 minutes
                result = orchestrator.quick_refresh()
            else:
                result = orchestrator.fetch_all_data_strategically()
            
            st.session_state.last_refresh_time = current_time
        
        st.session_state.last_refresh = datetime.now()
        update_kpis()
        
        return result
        
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
    st.subheader("🔄 Data Management")
    
    refresh_mode = st.radio(
        "Refresh Mode:",
        ["⚡ Quick (10-15s)", "🚀 Full Strategic (30-45s)", "🤖 Smart Adaptive"],
        index=2,
        help="Quick: Recent flights & delays only\nFull: Complete data refresh\nSmart: Chooses based on last refresh time"
    )
    
    if st.button("🔄 Refresh Live Data", type="primary", use_container_width=True):
        mode_map = {
            "⚡ Quick (10-15s)": "quick",
            "🚀 Full Strategic (30-45s)": "full",
            "🤖 Smart Adaptive": "smart"
        }
        
        with st.spinner("Fetching latest data..."):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Simulate progress
            for i in range(5):
                progress_bar.progress((i + 1) * 20)
                messages = [
                    "🌍 Connecting to aviation data sources...",
                    "✈️ Retrieving flight schedules...",
                    "🛩️ Fetching aircraft information...",
                    "⏱️ Updating delay statistics...",
                    "💾 Processing and storing data..."
                ]
                status_text.text(messages[i])
                time.sleep(0.5)
            
            result = refresh_data(mode_map[refresh_mode])
            progress_bar.progress(100)
            
            if result and result.get('success'):
                status_text.text("✅ Data refresh completed!")
                st.success(f"✅ Updated {result.get('flights_updated', result.get('flights_stored', 0))} flights in {result.get('time_seconds', 0):.1f}s")
                st.rerun()
            else:
                status_text.text("❌ Refresh failed")
                st.error("Failed to refresh data")
    
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
        airport_display = [f"{code} - {airports_df[airports_df['iata_code'] == code]['city'].iloc[0]}" 
                          for code in airport_options]
        
        selected_airport = st.selectbox(
            "Select Airport",
            options=airport_options,
            format_func=lambda x: f"{x} - {airports_df[airports_df['iata_code'] == x]['city'].iloc[0]}",
            index=0 if 'DEL' in airport_options else 0
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
    
    # Auto-refresh toggle
    st.session_state.auto_refresh = st.toggle("🔄 Auto-refresh (every 5 min)", value=False)

# ==================== MAIN CONTENT ====================
# Header
st.markdown('<h1 class="main-header">✈️ Air Tracker: Flight Analytics Dashboard</h1>', unsafe_allow_html=True)

# Update KPIs on initial load
if not st.session_state.kpi_data:
    update_kpis()

# ==================== DASHBOARD TABS ====================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🏠 Overview", 
    "📊 Flight Explorer", 
    "🏢 Airport Analytics", 
    "⏱️ Delay Intelligence", 
    "🔍 Advanced Queries",
    "📈 Live Monitor"
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
        st.markdown(f'<div class="metric-card" style="background: linear-gradient(135deg, {delay_color} 0%, #{delay_color[1:]}80 100%);">', unsafe_allow_html=True)
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
        # Color code status
        def color_status(status):
            if status in ['Arrived', 'Landed']:
                return 'background-color: #D1FAE5'
            elif status == 'Delayed':
                return 'background-color: #FEF3C7'
            elif status == 'Cancelled':
                return 'background-color: #FEE2E2'
            else:
                return ''
        
        styled_df = recent_flights.style.applymap(
            lambda x: color_status(x) if pd.notna(x) else '', 
            subset=['status']
        )
        st.dataframe(styled_df, use_container_width=True, height=300)
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
    
    # Build dynamic query
    query_parts = []
    params = []
    
    if search_query:
        if '-' in search_query and len(search_query.split('-')) == 2:
            # Route search (e.g., DEL-BOM)
            origin, dest = search_query.split('-')
            query_parts.append("(origin_iata = ? AND destination_iata = ?)")
            params.extend([origin.upper(), dest.upper()])
        elif search_query.upper() in [code.upper() for code in config.AIRPORT_CODES]:
            # Airport code search
            query_parts.append("(origin_iata = ? OR destination_iata = ?)")
            params.extend([search_query.upper(), search_query.upper()])
        else:
            # General search
            query_parts.append("(flight_number LIKE ? OR airline_name LIKE ?)")
            params.extend([f"%{search_query}%", f"%{search_query}%"])
    
    if selected_status != 'All':
        if selected_status == 'On Time':
            query_parts.append("status IN ('Arrived', 'Landed', 'On Time')")
        else:
            query_parts.append("status = ?")
            params.append(selected_status)
    
    # Date range filter
    query_parts.append("DATE(flight_date) BETWEEN ? AND ?")
    params.extend([start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')])
    
    # Execute query
    base_query = '''
    SELECT 
        flight_number,
        airline_name,
        origin_iata,
        destination_iata,
        scheduled_departure,
        scheduled_arrival,
        actual_departure,
        actual_arrival,
        status,
        aircraft_registration,
        flight_date
    FROM flights
    '''
    
    if query_parts:
        base_query += " WHERE " + " AND ".join(query_parts)
    
    base_query += " ORDER BY scheduled_departure DESC LIMIT 200"
    
    flights_df = st.session_state.db.execute_query(base_query, params, return_df=True)
    
    if flights_df is not None and not flights_df.empty:
        st.success(f"Found {len(flights_df)} flights matching your criteria")
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            on_time = len(flights_df[flights_df['status'].isin(['Arrived', 'Landed', 'On Time'])])
            st.metric("On Time", on_time)
        with col2:
            delayed = len(flights_df[flights_df['status'] == 'Delayed'])
            st.metric("Delayed", delayed)
        with col3:
            cancelled = len(flights_df[flights_df['status'] == 'Cancelled'])
            st.metric("Cancelled", cancelled)
        with col4:
            st.metric("Total", len(flights_df))
        
        # Display flights
        st.dataframe(
            flights_df,
            column_config={
                "flight_number": "Flight",
                "airline_name": "Airline",
                "origin_iata": "From",
                "destination_iata": "To",
                "scheduled_departure": "Scheduled",
                "status": st.column_config.TextColumn(
                    "Status",
                    help="Flight status",
                    width="small"
                )
            },
            use_container_width=True,
            height=400
        )
        
        # Export option
        csv = flights_df.to_csv(index=False)
        st.download_button(
            label="📥 Download Results (CSV)",
            data=csv,
            file_name=f"flight_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    else:
        st.info("No flights found matching your criteria. Try broadening your search.")
        
        # Show sample flights
        st.markdown("### Sample Flights")
        sample_flights = st.session_state.db.execute_query('''
            SELECT flight_number, airline_name, origin_iata, destination_iata, 
                   scheduled_departure, status
            FROM flights
            ORDER BY RANDOM()
            LIMIT 10
        ''', return_df=True)
        
        if sample_flights is not None and not sample_flights.empty:
            st.dataframe(sample_flights, use_container_width=True)

# Tab 3: Airport Analytics
with tab3:
    st.markdown('<h2 class="sub-header">Airport Analytics & Details</h2>', unsafe_allow_html=True)
    
    selected_airport = st.session_state.selected_airport
    
    # Airport overview
    airport_info = st.session_state.db.execute_query(
        '''
        SELECT name, city, country, continent, timezone, 
               latitude, longitude, last_updated
        FROM airport
        WHERE iata_code = ?
        ''',
        (selected_airport,),
        return_df=True
    )
    
    if airport_info is not None and not airport_info.empty:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("### Airport Information")
            info_cols = st.columns(2)
            
            with info_cols[0]:
                st.metric("Airport", airport_info.iloc[0]['name'])
                st.metric("City", airport_info.iloc[0]['city'])
                st.metric("Country", airport_info.iloc[0]['country'])
            
            with info_cols[1]:
                st.metric("Continent", airport_info.iloc[0]['continent'])
                st.metric("Timezone", airport_info.iloc[0]['timezone'])
                if airport_info.iloc[0]['last_updated']:
                    st.metric("Last Updated", 
                             pd.to_datetime(airport_info.iloc[0]['last_updated']).strftime('%Y-%m-%d'))
        
        with col2:
            # Show on map
            if pd.notna(airport_info.iloc[0]['latitude']) and pd.notna(airport_info.iloc[0]['longitude']):
                map_data = pd.DataFrame({
                    'lat': [airport_info.iloc[0]['latitude']],
                    'lon': [airport_info.iloc[0]['longitude']]
                })
                st.map(map_data, zoom=10)
        
        st.markdown("---")
        
        # Airport statistics
        st.markdown("### Airport Statistics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            departures_today = st.session_state.db.execute_query('''
                SELECT COUNT(*) FROM flights 
                WHERE origin_iata = ? AND DATE(flight_date) = DATE('now')
            ''', (selected_airport,))[0][0]
            st.metric("Today's Departures", departures_today)
        
        with col2:
            arrivals_today = st.session_state.db.execute_query('''
                SELECT COUNT(*) FROM flights 
                WHERE destination_iata = ? AND DATE(flight_date) = DATE('now')
            ''', (selected_airport,))[0][0]
            st.metric("Today's Arrivals", arrivals_today)
        
        with col3:
            avg_delay_result = st.session_state.db.execute_query('''
                SELECT AVG(avg_delay_min) FROM airport_delays 
                WHERE airport_iata = ? AND delay_date = DATE('now')
            ''', (selected_airport,))
            avg_delay = avg_delay_result[0][0] if avg_delay_result and avg_delay_result[0][0] else 0
            st.metric("Avg Delay Today", f"{avg_delay:.1f}m")
        
        with col4:
            top_destination = st.session_state.db.execute_query('''
                SELECT destination_iata, COUNT(*) as count
                FROM flights
                WHERE origin_iata = ?
                AND DATE(flight_date) >= DATE('now', '-7 days')
                GROUP BY destination_iata
                ORDER BY count DESC
                LIMIT 1
            ''', (selected_airport,), return_df=True)
            
            if top_destination is not None and not top_destination.empty:
                st.metric("Top Destination", top_destination.iloc[0]['destination_iata'])
            else:
                st.metric("Top Destination", "N/A")
        
        # Flight activity
        st.markdown("### Recent Flight Activity")
        
        flight_activity = st.session_state.db.execute_query('''
            SELECT 
                flight_number,
                airline_name,
                CASE 
                    WHEN origin_iata = ? THEN 'Departure'
                    ELSE 'Arrival'
                END as direction,
                CASE 
                    WHEN origin_iata = ? THEN destination_iata
                    ELSE origin_iata
                END as partner_airport,
                scheduled_departure,
                scheduled_arrival,
                status,
                aircraft_registration
            FROM flights
            WHERE (origin_iata = ? OR destination_iata = ?)
            AND DATE(flight_date) >= DATE('now', '-1 days')
            ORDER BY scheduled_departure DESC
            LIMIT 20
        ''', (selected_airport, selected_airport, selected_airport, selected_airport), return_df=True)
        
        if flight_activity is not None and not flight_activity.empty:
            st.dataframe(flight_activity, use_container_width=True)
        else:
            st.info(f"No recent flight activity for {selected_airport}")
    
    else:
        st.warning(f"No information available for airport {selected_airport}")
        st.info("Try refreshing the data or select a different airport.")

# Tab 4: Delay Intelligence
with tab4:
    st.markdown('<h2 class="sub-header">Delay Analysis & Intelligence</h2>', unsafe_allow_html=True)
    
    # Delay statistics
    delay_stats = st.session_state.db.execute_query('''
        SELECT 
            ad.airport_iata,
            a.name as airport_name,
            a.city,
            ad.delay_date,
            ad.total_flights,
            ad.delayed_flights,
            ad.avg_delay_min,
            ad.canceled_flights,
            ROUND((ad.delayed_flights * 100.0 / NULLIF(ad.total_flights, 0)), 2) as delay_percentage
        FROM airport_delays ad
        JOIN airport a ON ad.airport_iata = a.iata_code
        WHERE ad.delay_date >= DATE('now', '-7 days')
        ORDER BY ad.delay_date DESC, delay_percentage DESC
    ''', return_df=True)
    
    if delay_stats is not None and not delay_stats.empty:
        # Current delay status
        current_delays = delay_stats[delay_stats['delay_date'] == datetime.now().strftime('%Y-%m-%d')]
        
        if not current_delays.empty:
            st.markdown("### Current Delay Status")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                total_delayed = current_delays['delayed_flights'].sum()
                st.metric("Flights Delayed Right Now", f"{total_delayed:,}")
            
            with col2:
                avg_delay_all = current_delays['avg_delay_min'].mean()
                st.metric("Average Delay", f"{avg_delay_all:.1f} min")
            
            with col3:
                total_cancelled = current_delays['canceled_flights'].sum()
                st.metric("Cancelled Flights", f"{total_cancelled:,}")
        
        # Visualizations
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Most Delayed Airports (Today)")
            top_delayed = delay_stats[
                delay_stats['delay_date'] == datetime.now().strftime('%Y-%m-%d')
            ].sort_values('delay_percentage', ascending=False).head(10)
            
            if not top_delayed.empty:
                fig = px.bar(
                    top_delayed,
                    x='delay_percentage',
                    y='airport_name',
                    orientation='h',
                    color='avg_delay_min',
                    color_continuous