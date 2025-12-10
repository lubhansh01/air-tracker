import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from database import FlightDatabase
from api_handler import DataFetcher
import time

# Page configuration
st.set_page_config(
    page_title="Air Tracker: Flight Analytics",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize database
@st.cache_resource
def init_database():
    return FlightDatabase()

# Initialize session state
if 'db' not in st.session_state:
    st.session_state.db = init_database()
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = None

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #374151;
        margin-top: 1.5rem;
    }
    .metric-card {
        background-color: #F3F4F6;
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .stDataFrame {
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/824/824100.png", width=100)
    st.title("✈️ Air Tracker")
    st.markdown("---")
    
    # Refresh data button
    if st.button("🔄 Refresh Live Data", use_container_width=True):
        with st.spinner("Fetching latest flight data..."):
            fetcher = DataFetcher(st.session_state.db)
            fetcher.fetch_all_data()
            st.session_state.last_refresh = datetime.now()
            st.success("Data refreshed successfully!")
            st.rerun()
    
    if st.session_state.last_refresh:
        st.caption(f"Last refreshed: {st.session_state.last_refresh.strftime('%Y-%m-%d %H:%M:%S')}")
    
    st.markdown("---")
    
    # Filters
    st.subheader("Filters")
    
    # Date range filter
    today = datetime.now()
    date_range = st.date_input(
        "Select Date Range",
        value=(today - timedelta(days=7), today),
        max_value=today
    )
    
    # Airport filter
    airports_df = st.session_state.db.execute_query(
        "SELECT iata_code, name, city FROM airport ORDER BY city",
        return_df=True
    )
    
    if airports_df is not None and not airports_df.empty:
        selected_airports = st.multiselect(
            "Select Airports",
            options=airports_df['iata_code'].tolist(),
            format_func=lambda x: f"{x} - {airports_df[airports_df['iata_code'] == x]['city'].iloc[0]}"
        )
    else:
        selected_airports = []
    
    # Status filter
    status_options = ['All', 'On Time', 'Delayed', 'Cancelled', 'Diverted']
    selected_status = st.selectbox("Flight Status", status_options)
    
    st.markdown("---")
    st.markdown("### Project Info")
    st.info("""
    **Air Tracker: Flight Analytics**
    
    - Real-time flight data
    - Airport analytics
    - Delay analysis
    - Route optimization
    """)

# Main content
st.markdown('<h1 class="main-header">✈️ Air Tracker: Flight Analytics Dashboard</h1>', unsafe_allow_html=True)

# Dashboard tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🏠 Dashboard", 
    "📊 Flight Analytics", 
    "🏢 Airport Details", 
    "⏱️ Delay Analysis", 
    "🔍 Advanced Queries"
])

with tab1:
    # KPI Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_airports = st.session_state.db.execute_query(
            "SELECT COUNT(*) FROM airport"
        )[0][0]
        st.metric("Total Airports", total_airports)
    
    with col2:
        total_flights = st.session_state.db.execute_query(
            "SELECT COUNT(*) FROM flights"
        )[0][0]
        st.metric("Total Flights", total_flights)
    
    with col3:
        avg_delay = st.session_state.db.execute_query(
            "SELECT AVG(avg_delay_min) FROM airport_delays WHERE avg_delay_min > 0"
        )[0][0]
        st.metric("Avg Delay (mins)", f"{avg_delay:.1f}" if avg_delay else "N/A")
    
    with col4:
        unique_aircraft = st.session_state.db.execute_query(
            "SELECT COUNT(DISTINCT aircraft_registration) FROM flights WHERE aircraft_registration IS NOT NULL"
        )[0][0]
        st.metric("Unique Aircraft", unique_aircraft)
    
    st.markdown("---")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<h3 class="sub-header">Flight Status Distribution</h3>', unsafe_allow_html=True)
        status_data = st.session_state.db.execute_query('''
            SELECT 
                CASE 
                    WHEN status = 'Arrived' THEN 'On Time'
                    WHEN status = 'Delayed' THEN 'Delayed'
                    WHEN status = 'Cancelled' THEN 'Cancelled'
                    ELSE 'Other'
                END as status_category,
                COUNT(*) as count
            FROM flights
            GROUP BY status_category
            ORDER BY count DESC
        ''', return_df=True)
        
        if status_data is not None and not status_data.empty:
            fig = px.pie(
                status_data, 
                values='count', 
                names='status_category',
                color_discrete_sequence=px.colors.sequential.RdBu
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown('<h3 class="sub-header">Top Airlines by Flights</h3>', unsafe_allow_html=True)
        airline_data = st.session_state.db.execute_query('''
            SELECT airline_name, COUNT(*) as flight_count
            FROM flights
            WHERE airline_name IS NOT NULL AND airline_name != ''
            GROUP BY airline_name
            ORDER BY flight_count DESC
            LIMIT 10
        ''', return_df=True)
        
        if airline_data is not None and not airline_data.empty:
            fig = px.bar(
                airline_data,
                x='flight_count',
                y='airline_name',
                orientation='h',
                color='flight_count',
                color_continuous_scale='Blues'
            )
            fig.update_layout(xaxis_title="Number of Flights", yaxis_title="Airline")
            st.plotly_chart(fig, use_container_width=True)
    
    # Recent flights table
    st.markdown('<h3 class="sub-header">Recent Flights</h3>', unsafe_allow_html=True)
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
        LIMIT 20
    ''', return_df=True)
    
    if recent_flights is not None and not recent_flights.empty:
        st.dataframe(recent_flights, use_container_width=True)

with tab2:
    st.markdown('<h2 class="sub-header">Flight Analytics & Search</h2>', unsafe_allow_html=True)
    
    # Search section
    col1, col2 = st.columns([2, 1])
    
    with col1:
        search_query = st.text_input("🔍 Search flights by number or airline:", placeholder="e.g., AI101 or Air India")
    
    with col2:
        flight_date = st.date_input("Filter by date", value=datetime.now())
    
    # Build query based on filters
    query_conditions = []
    query_params = []
    
    if search_query:
        query_conditions.append("(flight_number LIKE ? OR airline_name LIKE ?)")
        query_params.extend([f"%{search_query}%", f"%{search_query}%"])
    
    if selected_airports:
        placeholders = ','.join(['?' for _ in selected_airports])
        query_conditions.append(f"(origin_iata IN ({placeholders}) OR destination_iata IN ({placeholders}))")
        query_params.extend(selected_airports)
        query_params.extend(selected_airports)
    
    if selected_status != 'All':
        if selected_status == 'On Time':
            query_conditions.append("status = 'Arrived'")
        else:
            query_conditions.append(f"status = '{selected_status}'")
    
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
        aircraft_registration
    FROM flights
    WHERE 1=1
    '''
    
    if query_conditions:
        base_query += " AND " + " AND ".join(query_conditions)
    
    base_query += " ORDER BY scheduled_departure DESC LIMIT 100"
    
    flights = st.session_state.db.execute_query(base_query, query_params, return_df=True)
    
    if flights is not None and not flights.empty:
        st.dataframe(flights, use_container_width=True)
        
        # Download button
        csv = flights.to_csv(index=False)
        st.download_button(
            label="📥 Download Flight Data (CSV)",
            data=csv,
            file_name=f"flight_data_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
    else:
        st.info("No flights found matching your criteria.")

with tab3:
    st.markdown('<h2 class="sub-header">Airport Details</h2>', unsafe_allow_html=True)
    
    # Airport selector
    airports = st.session_state.db.execute_query(
        "SELECT iata_code, name, city, country FROM airport ORDER BY country, city",
        return_df=True
    )
    
    if airports is not None and not airports.empty:
        selected_airport = st.selectbox(
            "Select Airport",
            options=airports['iata_code'].tolist(),
            format_func=lambda x: f"{x} - {airports[airports['iata_code'] == x]['name'].iloc[0]}"
        )
        
        if selected_airport:
            # Airport details
            airport_details = st.session_state.db.execute_query(
                '''
                SELECT name, city, country, continent, timezone, 
                       latitude, longitude, last_updated
                FROM airport
                WHERE iata_code = ?
                ''',
                (selected_airport,),
                return_df=True
            )
            
            if airport_details is not None and not airport_details.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("### Airport Information")
                    for col in ['name', 'city', 'country', 'continent', 'timezone']:
                        if col in airport_details.columns:
                            st.write(f"**{col.title()}:** {airport_details.iloc[0][col]}")
                
                with col2:
                    st.markdown("### Location")
                    if 'latitude' in airport_details.columns and 'longitude' in airport_details.columns:
                        location_df = pd.DataFrame({
                            'lat': [airport_details.iloc[0]['latitude']],
                            'lon': [airport_details.iloc[0]['longitude']]
                        })
                        st.map(location_df, zoom=10)
                
                # Airport statistics
                st.markdown("### Airport Statistics")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    departing_flights = st.session_state.db.execute_query(
                        '''
                        SELECT COUNT(*) FROM flights 
                        WHERE origin_iata = ? AND DATE(scheduled_departure) = DATE('now')
                        ''',
                        (selected_airport,)
                    )[0][0]
                    st.metric("Today's Departures", departing_flights)
                
                with col2:
                    arriving_flights = st.session_state.db.execute_query(
                        '''
                        SELECT COUNT(*) FROM flights 
                        WHERE destination_iata = ? AND DATE(scheduled_arrival) = DATE('now')
                        ''',
                        (selected_airport,)
                    )[0][0]
                    st.metric("Today's Arrivals", arriving_flights)
                
                with col3:
                    delay_stats = st.session_state.db.execute_query(
                        '''
                        SELECT avg_delay_min FROM airport_delays 
                        WHERE airport_iata = ? ORDER BY delay_date DESC LIMIT 1
                        ''',
                        (selected_airport,)
                    )
                    avg_delay = delay_stats[0][0] if delay_stats else 0
                    st.metric("Avg Delay (mins)", f"{avg_delay:.1f}" if avg_delay else "N/A")
                
                # Flight schedule
                st.markdown("### Recent Flight Activity")
                
                today_flights = st.session_state.db.execute_query('''
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
                        END as other_airport,
                        scheduled_departure,
                        scheduled_arrival,
                        status
                    FROM flights
                    WHERE (origin_iata = ? OR destination_iata = ?)
                    ORDER BY scheduled_departure DESC
                    LIMIT 20
                ''', (selected_airport, selected_airport, selected_airport, selected_airport), return_df=True)
                
                if today_flights is not None and not today_flights.empty:
                    st.dataframe(today_flights, use_container_width=True)

with tab4:
    st.markdown('<h2 class="sub-header">Delay Analysis</h2>', unsafe_allow_html=True)
    
    # Delay statistics
    delay_data = st.session_state.db.execute_query('''
        SELECT 
            a.iata_code,
            a.name,
            a.city,
            d.delay_date,
            d.total_flights,
            d.delayed_flights,
            d.avg_delay_min,
            d.canceled_flights,
            ROUND((d.delayed_flights * 100.0 / NULLIF(d.total_flights, 0)), 2) as delay_percentage
        FROM airport_delays d
        JOIN airport a ON d.airport_iata = a.iata_code
        ORDER BY d.delay_date DESC, delay_percentage DESC
        LIMIT 50
    ''', return_df=True)
    
    if delay_data is not None and not delay_data.empty:
        # Top delayed airports
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Most Delayed Airports")
            top_delayed = delay_data.sort_values('delay_percentage', ascending=False).head(10)
            fig = px.bar(
                top_delayed,
                x='delay_percentage',
                y='name',
                orientation='h',
                color='avg_delay_min',
                color_continuous_scale='Reds',
                labels={'delay_percentage': 'Delay %', 'name': 'Airport', 'avg_delay_min': 'Avg Delay (mins)'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### Delay Trends")
            fig = px.line(
                delay_data,
                x='delay_date',
                y='delay_percentage',
                color='name',
                markers=True,
                labels={'delay_date': 'Date', 'delay_percentage': 'Delay %', 'name': 'Airport'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Delay statistics table
        st.markdown("### Detailed Delay Statistics")
        st.dataframe(delay_data, use_container_width=True)

with tab5:
    st.markdown('<h2 class="sub-header">Advanced Analytics Queries</h2>', unsafe_allow_html=True)
    
    # Query selection
    query_options = {
        "Busiest Routes": '''
            SELECT 
                origin_iata,
                destination_iata,
                COUNT(*) as flight_count
            FROM flights
            GROUP BY origin_iata, destination_iata
            HAVING flight_count > 5
            ORDER BY flight_count DESC
            LIMIT 20
        ''',
        "Aircraft Utilization": '''
            SELECT 
                a.model,
                a.manufacturer,
                COUNT(f.flight_id) as total_flights,
                COUNT(DISTINCT f.airline_name) as airlines_used
            FROM aircraft a
            LEFT JOIN flights f ON a.registration = f.aircraft_registration
            WHERE a.model IS NOT NULL
            GROUP BY a.model, a.manufacturer
            ORDER BY total_flights DESC
            LIMIT 15
        ''',
        "International vs Domestic": '''
            SELECT 
                CASE 
                    WHEN o.country = d.country THEN 'Domestic'
                    ELSE 'International'
                END as flight_type,
                COUNT(*) as flight_count,
                ROUND(AVG(
                    CASE 
                        WHEN f.status = 'Delayed' THEN 1 
                        ELSE 0 
                    END) * 100, 2) as delay_percentage
            FROM flights f
            JOIN airport o ON f.origin_iata = o.iata_code
            JOIN airport d ON f.destination_iata = d.iata_code
            GROUP BY flight_type
        ''',
        "Airline Performance": '''
            SELECT 
                airline_name,
                COUNT(*) as total_flights,
                SUM(CASE WHEN status = 'Delayed' THEN 1 ELSE 0 END) as delayed_flights,
                SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_flights,
                ROUND((SUM(CASE WHEN status = 'Delayed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as delay_percentage
            FROM flights
            WHERE airline_name IS NOT NULL AND airline_name != ''
            GROUP BY airline_name
            HAVING total_flights > 10
            ORDER BY delay_percentage DESC
            LIMIT 15
        '''
    }
    
    selected_query = st.selectbox("Select Analysis", list(query_options.keys()))
    
    if st.button("Run Analysis"):
        with st.spinner("Running analysis..."):
            results = st.session_state.db.execute_query(
                query_options[selected_query], 
                return_df=True
            )
            
            if results is not None and not results.empty:
                st.dataframe(results, use_container_width=True)
                
                # Visualization
                if selected_query == "International vs Domestic":
                    fig = px.pie(results, values='flight_count', names='flight_type')
                    st.plotly_chart(fig, use_container_width=True)
                elif selected_query in ["Busiest Routes", "Aircraft Utilization", "Airline Performance"]:
                    if 'flight_count' in results.columns or 'total_flights' in results.columns:
                        fig = px.bar(
                            results.head(10),
                            x=results.columns[2] if 'delay_percentage' in results.columns else results.columns[1],
                            y=results.columns[0],
                            orientation='h'
                        )
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No results found for this analysis.")

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666;">
    <p>✈️ Air Tracker: Flight Analytics Dashboard | Powered by AeroDataBox API</p>
    <p>Data updates every 30 minutes | Last update: {}</p>
</div>
""".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")), unsafe_allow_html=True)