import streamlit as st
import pandas as pd
from app.utils import (
    get_dataframe_from_query, display_metrics,
    plot_bar_chart, plot_pie_chart, plot_geographic_map,
    init_database
)
from database.queries import FlightQueries

def home_page():
    st.title("✈️ Air Tracker: Flight Analytics Dashboard")
    
    # Initialize database
    db = init_database()
    if not db:
        st.warning("Please ensure the database is set up and running")
        return
    
    # Quick stats row
    st.subheader("📊 Quick Statistics")
    
    # Get airport summary
    airport_summary = get_dataframe_from_query(FlightQueries.get_airport_summary())
    flight_summary = get_dataframe_from_query(FlightQueries.get_flight_summary())
    
    if not airport_summary.empty and not flight_summary.empty:
        metrics = {
            "Total Airports": int(airport_summary['total_airports'].iloc[0]),
            "Countries Covered": int(airport_summary['countries_covered'].iloc[0]),
            "Total Flights": int(flight_summary['total_flights'].iloc[0]),
            "Airlines": int(flight_summary['airlines'].iloc[0]),
            "Unique Aircraft": int(flight_summary['unique_aircraft'].iloc[0]),
            "Avg Delay %": f"{flight_summary['avg_delay_percentage'].iloc[0]:.1f}%"
        }
        display_metrics(metrics)
    
    # Two column layout
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🌍 Airport Locations")
        airports_df = get_dataframe_from_query("SELECT * FROM airport")
        if not airports_df.empty:
            fig = plot_geographic_map(airports_df)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📈 Top Airlines by Flights")
        query = """
            SELECT airline_code, COUNT(*) as flight_count 
            FROM flights 
            WHERE airline_code IS NOT NULL 
            GROUP BY airline_code 
            ORDER BY flight_count DESC 
            LIMIT 10
        """
        airlines_df = get_dataframe_from_query(query)
        if not airlines_df.empty:
            fig = plot_bar_chart(airlines_df, 'airline_code', 'flight_count', 
                                'Top Airlines by Number of Flights')
            st.plotly_chart(fig, use_container_width=True)
    
    # Second row
    col3, col4 = st.columns(2)
    
    with col3:
        st.subheader("🛫 Flight Status Distribution")
        query = """
            SELECT status, COUNT(*) as count 
            FROM flights 
            GROUP BY status 
            ORDER BY count DESC
        """
        status_df = get_dataframe_from_query(query)
        if not status_df.empty:
            fig = plot_pie_chart(status_df, 'status', 'count', 'Flight Status Distribution')
            st.plotly_chart(fig, use_container_width=True)
    
    with col4:
        st.subheader("⏰ Top Delayed Airports")
        delay_df = get_dataframe_from_query(FlightQueries.QUERY_11)
        if not delay_df.empty:
            # Get top 10
            top_delays = delay_df.head(10).copy()
            fig = plot_bar_chart(top_delays, 'airport_name', 'delay_percentage',
                                'Top 10 Airports by Delay Percentage')
            st.plotly_chart(fig, use_container_width=True)
    
    # Recent flights table
    st.subheader("🔄 Recent Flights")
    query = """
        SELECT 
            f.flight_number,
            f.airline_code,
            o.name as origin,
            d.name as destination,
            f.status,
            DATE(f.scheduled_departure) as date,
            TIME(f.scheduled_departure) as time
        FROM flights f
        LEFT JOIN airport o ON f.origin_iata = o.iata_code
        LEFT JOIN airport d ON f.destination_iata = d.iata_code
        ORDER BY f.scheduled_departure DESC
        LIMIT 10
    """
    recent_flights = get_dataframe_from_query(query)
    if not recent_flights.empty:
        st.dataframe(recent_flights, use_container_width=True, 
                    column_config={
                        "status": st.column_config.TextColumn(
                            "Status",
                            help="Flight status"
                        )
                    })
    
    # Query execution section
    st.subheader("🔍 Run Custom Queries")
    
    query_options = {
        "Top Destination Airports": FlightQueries.QUERY_4,
        "Aircraft with Most Flights": FlightQueries.QUERY_2,
        "Airports with No Arrivals": FlightQueries.QUERY_7,
        "Flight Type Analysis": FlightQueries.QUERY_5,
        "Airline Performance": FlightQueries.QUERY_8
    }
    
    selected_query = st.selectbox("Select a pre-defined query:", list(query_options.keys()))
    
    if st.button("Execute Query"):
        with st.spinner("Running query..."):
            result_df = get_dataframe_from_query(query_options[selected_query])
            if not result_df.empty:
                st.dataframe(result_df, use_container_width=True)
                
                # Show chart for certain queries
                if selected_query == "Top Destination Airports":
                    fig = plot_bar_chart(result_df, 'airport_name', 'arriving_flights',
                                        'Top Destination Airports')
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data returned from query")

if __name__ == "__main__":
    home_page()