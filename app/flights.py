import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from app.utils import (
    get_dataframe_from_query, plot_bar_chart,
    plot_line_chart, format_flight_status
)
from database.queries import FlightQueries

def flights_page():
    st.title("🛫 Flight Operations")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        status_filter = st.multiselect(
            "Flight Status",
            options=['Scheduled', 'On Time', 'Delayed', 'Cancelled', 'Completed'],
            default=['Scheduled', 'On Time', 'Delayed']
        )
    
    with col2:
        airline_query = "SELECT DISTINCT airline_code FROM flights WHERE airline_code IS NOT NULL"
        airlines_df = get_dataframe_from_query(airline_query)
        airline_options = ['All'] + airlines_df['airline_code'].tolist() if not airlines_df.empty else ['All']
        airline_filter = st.selectbox("Airline", options=airline_options)
    
    with col3:
        date_range = st.date_input(
            "Date Range",
            value=[datetime.now() - timedelta(days=7), datetime.now()],
            max_value=datetime.now()
        )
    
    # Search flight
    st.subheader("🔍 Search Flights")
    search_col1, search_col2 = st.columns([2, 1])
    
    with search_col1:
        flight_search = st.text_input("Search by Flight Number", placeholder="e.g., AI101, 6E205")
    
    with search_col2:
        search_button = st.button("Search", use_container_width=True)
    
    # Build query based on filters
    query = """
        SELECT 
            f.flight_id,
            f.flight_number,
            f.airline_code,
            f.aircraft_registration,
            a.model as aircraft_model,
            o.name as origin_airport,
            d.name as destination_airport,
            f.scheduled_departure,
            f.actual_departure,
            f.scheduled_arrival,
            f.actual_arrival,
            f.status,
            f.direction,
            TIMESTAMPDIFF(MINUTE, f.scheduled_departure, f.actual_departure) as departure_delay,
            TIMESTAMPDIFF(MINUTE, f.scheduled_arrival, f.actual_arrival) as arrival_delay
        FROM flights f
        LEFT JOIN aircraft a ON f.aircraft_registration = a.registration
        LEFT JOIN airport o ON f.origin_iata = o.iata_code
        LEFT JOIN airport d ON f.destination_iata = d.iata_code
        WHERE 1=1
    """
    
    params = []
    
    if status_filter:
        status_placeholders = ','.join(['%s'] * len(status_filter))
        query += f" AND f.status IN ({status_placeholders})"
        params.extend(status_filter)
    
    if airline_filter != 'All':
        query += " AND f.airline_code = %s"
        params.append(airline_filter)
    
    if len(date_range) == 2:
        query += " AND DATE(f.scheduled_departure) BETWEEN %s AND %s"
        params.extend([date_range[0], date_range[1]])
    
    if flight_search and search_button:
        query += " AND f.flight_number LIKE %s"
        params.append(f"%{flight_search}%")
    
    query += " ORDER BY f.scheduled_departure DESC"
    
    # Execute query
    flights_df = get_dataframe_from_query(query, params)
    
    if not flights_df.empty:
        # Display metrics
        total_flights = len(flights_df)
        delayed_flights = len(flights_df[flights_df['status'] == 'Delayed'])
        cancelled_flights = len(flights_df[flights_df['status'] == 'Cancelled'])
        
        metric_cols = st.columns(4)
        with metric_cols[0]:
            st.metric("Total Flights", total_flights)
        with metric_cols[1]:
            st.metric("Delayed", delayed_flights)
        with metric_cols[2]:
            st.metric("Cancelled", cancelled_flights)
        with metric_cols[3]:
            on_time_rate = ((total_flights - delayed_flights - cancelled_flights) / total_flights * 100) if total_flights > 0 else 0
            st.metric("On Time Rate", f"{on_time_rate:.1f}%")
        
        # Format status with emojis
        flights_df['status_formatted'] = flights_df['status'].apply(format_flight_status)
        
        # Display flights table
        st.subheader(f"📋 Flights ({len(flights_df)} found)")
        
        # Select columns to display
        display_columns = [
            'flight_number', 'airline_code', 'origin_airport', 'destination_airport',
            'scheduled_departure', 'status_formatted', 'departure_delay'
        ]
        
        display_df = flights_df[display_columns].copy()
        display_df.columns = ['Flight', 'Airline', 'Origin', 'Destination', 'Scheduled', 'Status', 'Delay (min)']
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Status": st.column_config.TextColumn("Status", width="small"),
                "Delay (min)": st.column_config.NumberColumn("Delay (min)", format="%d")
            }
        )
        
        # Flight analytics
        st.subheader("📈 Flight Analytics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Flights by status
            status_counts = flights_df['status'].value_counts().reset_index()
            status_counts.columns = ['status', 'count']
            fig = plot_bar_chart(status_counts, 'status', 'count', 'Flights by Status')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Flights by hour of day
            flights_df['hour'] = pd.to_datetime(flights_df['scheduled_departure']).dt.hour
            hourly_counts = flights_df.groupby('hour').size().reset_index()
            hourly_counts.columns = ['hour', 'count']
            fig = plot_line_chart(hourly_counts, 'hour', 'count', 'Flights by Hour of Day')
            st.plotly_chart(fig, use_container_width=True)
        
        # Detailed view
        st.subheader("🔍 Flight Details")
        if not flights_df.empty:
            selected_flight = st.selectbox(
                "Select a flight for details",
                options=flights_df['flight_number'].unique()
            )
            
            if selected_flight:
                flight_details = flights_df[flights_df['flight_number'] == selected_flight].iloc[0]
                
                detail_cols = st.columns(2)
                
                with detail_cols[0]:
                    st.info("**Flight Information**")
                    st.write(f"**Flight Number:** {flight_details['flight_number']}")
                    st.write(f"**Airline:** {flight_details['airline_code']}")
                    st.write(f"**Aircraft:** {flight_details['aircraft_registration']} ({flight_details['aircraft_model']})")
                    st.write(f"**Status:** {format_flight_status(flight_details['status'])}")
                    st.write(f"**Direction:** {flight_details['direction'].title()}")
                
                with detail_cols[1]:
                    st.info("**Route Information**")
                    st.write(f"**Origin:** {flight_details['origin_airport']}")
                    st.write(f"**Destination:** {flight_details['destination_airport']}")
                    st.write(f"**Scheduled Departure:** {flight_details['scheduled_departure']}")
                    st.write(f"**Actual Departure:** {flight_details['actual_departure'] or 'N/A'}")
                    st.write(f"**Departure Delay:** {flight_details['departure_delay'] or 0} minutes")
    else:
        st.info("No flights found matching the criteria")

if __name__ == "__main__":
    flights_page()