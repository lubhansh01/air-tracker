import streamlit as st
import pandas as pd
from app.utils import (
    get_dataframe_from_query, plot_bar_chart,
    plot_pie_chart, plot_geographic_map
)
from database.queries import FlightQueries

def airports_page():
    st.title("🏢 Airport Analytics")
    
    # Get all airports
    airports_df = get_dataframe_from_query("SELECT * FROM airport ORDER BY country, city")
    
    if airports_df.empty:
        st.warning("No airport data available")
        return
    
    # Airport selector
    st.subheader("📍 Select Airport")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        airport_options = airports_df.apply(
            lambda x: f"{x['iata_code']} - {x['name']} ({x['city']}, {x['country']})", 
            axis=1
        ).tolist()
        
        selected_airport_str = st.selectbox(
            "Choose an airport",
            options=airport_options,
            index=0
        )
        
        # Extract IATA code from selection
        selected_iata = selected_airport_str.split(' - ')[0]
    
    with col2:
        st.metric("Total Airports", len(airports_df))
    
    if selected_iata:
        # Get airport details
        airport_details = airports_df[airports_df['iata_code'] == selected_iata].iloc[0]
        
        # Display airport information
        st.subheader(f"📋 {airport_details['name']} ({selected_iata})")
        
        info_cols = st.columns(3)
        
        with info_cols[0]:
            st.info("**Location**")
            st.write(f"**City:** {airport_details['city']}")
            st.write(f"**Country:** {airport_details['country']}")
            st.write(f"**Continent:** {airport_details['continent']}")
        
        with info_cols[1]:
            st.info("**Coordinates**")
            st.write(f"**Latitude:** {airport_details['latitude']:.4f}")
            st.write(f"**Longitude:** {airport_details['longitude']:.4f}")
            st.write(f"**Timezone:** {airport_details['timezone']}")
        
        with info_cols[2]:
            st.info("**Codes**")
            st.write(f"**IATA:** {airport_details['iata_code']}")
            st.write(f"**ICAO:** {airport_details['icao_code']}")
        
        # Airport statistics
        st.subheader("📊 Airport Statistics")
        
        # Get flight statistics for selected airport
        query_departures = f"""
            SELECT 
                COUNT(*) as total_departures,
                SUM(CASE WHEN status = 'Delayed' THEN 1 ELSE 0 END) as delayed_departures,
                SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_departures
            FROM flights
            WHERE origin_iata = '{selected_iata}'
        """
        
        query_arrivals = f"""
            SELECT 
                COUNT(*) as total_arrivals,
                SUM(CASE WHEN status = 'Delayed' THEN 1 ELSE 0 END) as delayed_arrivals,
                SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_arrivals
            FROM flights
            WHERE destination_iata = '{selected_iata}'
        """
        
        departures_df = get_dataframe_from_query(query_departures)
        arrivals_df = get_dataframe_from_query(query_arrivals)
        
        # Get delay statistics
        query_delays = f"""
            SELECT * FROM airport_delays 
            WHERE airport_iata = '{selected_iata}'
            ORDER BY delay_date DESC
            LIMIT 7
        """
        delays_df = get_dataframe_from_query(query_delays)
        
        # Display metrics
        if not departures_df.empty and not arrivals_df.empty:
            stat_cols = st.columns(4)
            
            with stat_cols[0]:
                st.metric("Total Departures", int(departures_df['total_departures'].iloc[0]))
            
            with stat_cols[1]:
                st.metric("Total Arrivals", int(arrivals_df['total_arrivals'].iloc[0]))
            
            with stat_cols[2]:
                if departures_df['total_departures'].iloc[0] > 0:
                    delay_percentage = (departures_df['delayed_departures'].iloc[0] / 
                                       departures_df['total_departures'].iloc[0] * 100)
                    st.metric("Departure Delay %", f"{delay_percentage:.1f}%")
                else:
                    st.metric("Departure Delay %", "0%")
            
            with stat_cols[3]:
                if not delays_df.empty:
                    avg_delay = delays_df['avg_delay_min'].mean()
                    st.metric("Avg Delay (min)", f"{avg_delay:.0f}")
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🛫 Top Destinations")
            query_top_dest = f"""
                SELECT 
                    d.name as destination,
                    COUNT(*) as flights_count
                FROM flights f
                JOIN airport d ON f.destination_iata = d.iata_code
                WHERE f.origin_iata = '{selected_iata}'
                GROUP BY f.destination_iata, d.name
                ORDER BY flights_count DESC
                LIMIT 10
            """
            top_dest_df = get_dataframe_from_query(query_top_dest)
            
            if not top_dest_df.empty:
                fig = plot_bar_chart(top_dest_df, 'destination', 'flights_count',
                                    f'Top Destinations from {selected_iata}')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No departure data available")
        
        with col2:
            st.subheader("🛬 Top Origins")
            query_top_orig = f"""
                SELECT 
                    o.name as origin,
                    COUNT(*) as flights_count
                FROM flights f
                JOIN airport o ON f.origin_iata = o.iata_code
                WHERE f.destination_iata = '{selected_iata}'
                GROUP BY f.origin_iata, o.name
                ORDER BY flights_count DESC
                LIMIT 10
            """
            top_orig_df = get_dataframe_from_query(query_top_orig)
            
            if not top_orig_df.empty:
                fig = plot_bar_chart(top_orig_df, 'origin', 'flights_count',
                                    f'Top Origins to {selected_iata}')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No arrival data available")
        
        # Delay trends
        st.subheader("⏰ Delay Trends")
        
        if not delays_df.empty:
            delays_df['delay_date'] = pd.to_datetime(delays_df['delay_date'])
            delays_df = delays_df.sort_values('delay_date')
            
            fig = plot_line_chart(delays_df, 'delay_date', 'avg_delay_min',
                                 'Average Delay (minutes) Over Time')
            st.plotly_chart(fig, use_container_width=True)
            
            # Delay metrics
            delay_cols = st.columns(4)
            with delay_cols[0]:
                avg_delay = delays_df['avg_delay_min'].mean()
                st.metric("Average Delay", f"{avg_delay:.0f} min")
            
            with delay_cols[1]:
                max_delay = delays_df['avg_delay_min'].max()
                st.metric("Maximum Delay", f"{max_delay:.0f} min")
            
            with delay_cols[2]:
                total_flights = delays_df['total_flights'].sum()
                st.metric("Total Flights", total_flights)
            
            with delay_cols[3]:
                delayed_flights = delays_df['delayed_flights'].sum()
                delay_percentage = (delayed_flights / total_flights * 100) if total_flights > 0 else 0
                st.metric("Delay Rate", f"{delay_percentage:.1f}%")
        else:
            st.info("No delay statistics available")
    
    # All airports view
    st.subheader("🌐 All Airports Overview")
    
    # Filters for all airports
    filter_col1, filter_col2 = st.columns(2)
    
    with filter_col1:
        continent_filter = st.multiselect(
            "Filter by Continent",
            options=airports_df['continent'].unique(),
            default=airports_df['continent'].unique()
        )
    
    with filter_col2:
        country_filter = st.multiselect(
            "Filter by Country",
            options=airports_df['country'].unique(),
            default=airports_df['country'].unique()
        )
    
    # Apply filters
    filtered_airports = airports_df.copy()
    if continent_filter:
        filtered_airports = filtered_airports[filtered_airports['continent'].isin(continent_filter)]
    if country_filter:
        filtered_airports = filtered_airports[filtered_airports['country'].isin(country_filter)]
    
    # Display airports table
    st.dataframe(
        filtered_airports[['iata_code', 'name', 'city', 'country', 'continent', 'timezone']],
        use_container_width=True,
        hide_index=True
    )
    
    # Airport distribution by country
    st.subheader("🗺️ Airport Distribution")
    
    country_dist = filtered_airports['country'].value_counts().reset_index()
    country_dist.columns = ['country', 'airport_count']
    
    if not country_dist.empty:
        fig = plot_bar_chart(country_dist.head(10), 'country', 'airport_count',
                            'Top 10 Countries by Number of Airports')
        st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    airports_page()