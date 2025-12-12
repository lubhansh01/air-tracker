import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from app.utils import get_dataframe_from_query, plot_bar_chart, plot_line_chart
from database.queries import FlightQueries

def analytics_page():
    st.title("📊 Advanced Analytics")
    
    # Tabs for different analytics
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Performance Metrics",
        "🛩️ Aircraft Analytics",
        "🌐 Route Analysis",
        "📋 Project Queries"
    ])
    
    with tab1:
        st.subheader("Flight Performance Metrics")
        
        # On-time performance by airline
        query_on_time = """
            SELECT 
                airline_code,
                COUNT(*) as total_flights,
                SUM(CASE WHEN status IN ('On Time', 'Completed') THEN 1 ELSE 0 END) as on_time_flights,
                ROUND(
                    (SUM(CASE WHEN status IN ('On Time', 'Completed') THEN 1 ELSE 0 END) * 100.0 / 
                    COUNT(*)), 2
                ) as on_time_percentage
            FROM flights
            WHERE airline_code IS NOT NULL
            GROUP BY airline_code
            HAVING total_flights > 10
            ORDER BY on_time_percentage DESC
        """
        
        on_time_df = get_dataframe_from_query(query_on_time)
        
        if not on_time_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Top performers
                top_performers = on_time_df.head(10)
                fig = plot_bar_chart(top_performers, 'airline_code', 'on_time_percentage',
                                    'Top 10 Airlines by On-Time Performance')
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Bottom performers
                bottom_performers = on_time_df.tail(10).sort_values('on_time_percentage')
                fig = plot_bar_chart(bottom_performers, 'airline_code', 'on_time_percentage',
                                    'Bottom 10 Airlines by On-Time Performance')
                st.plotly_chart(fig, use_container_width=True)
            
            # Detailed table
            st.subheader("Detailed Performance Metrics")
            display_df = on_time_df.copy()
            display_df.columns = ['Airline', 'Total Flights', 'On-Time Flights', 'On-Time %']
            st.dataframe(display_df, use_container_width=True)
        
        # Delay causes analysis
        st.subheader("Delay Analysis")
        
        query_delays = """
            SELECT 
                status,
                COUNT(*) as count,
                AVG(TIMESTAMPDIFF(MINUTE, scheduled_departure, actual_departure)) as avg_delay_minutes
            FROM flights
            WHERE status = 'Delayed' AND actual_departure IS NOT NULL
            GROUP BY status
        """
        
        delays_df = get_dataframe_from_query(query_delays)
        
        if not delays_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Average Delay", f"{delays_df['avg_delay_minutes'].iloc[0]:.0f} minutes")
            
            with col2:
                st.metric("Total Delayed Flights", int(delays_df['count'].iloc[0]))
    
    with tab2:
        st.subheader("Aircraft Utilization Analytics")
        
        # Query 1: Total flights per aircraft model
        st.subheader("1. Flights per Aircraft Model")
        query1_df = get_dataframe_from_query(FlightQueries.QUERY_1)
        
        if not query1_df.empty:
            fig = plot_bar_chart(query1_df.head(10), 'model', 'flight_count',
                                'Top 10 Aircraft Models by Number of Flights')
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(query1_df, use_container_width=True)
        
        # Query 2: Aircraft with more than 5 flights
        st.subheader("2. Aircraft with > 5 Flights")
        query2_df = get_dataframe_from_query(FlightQueries.QUERY_2)
        
        if not query2_df.empty:
            fig = px.scatter(query2_df, x='model', y='flight_count', size='flight_count',
                            hover_data=['registration'], title='Aircraft Flight Count Distribution')
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(query2_df, use_container_width=True)
        
        # Aircraft by manufacturer
        st.subheader("Aircraft by Manufacturer")
        query_manufacturer = """
            SELECT 
                manufacturer,
                COUNT(DISTINCT registration) as aircraft_count,
                COUNT(f.flight_id) as total_flights
            FROM aircraft a
            LEFT JOIN flights f ON a.registration = f.aircraft_registration
            WHERE manufacturer IS NOT NULL
            GROUP BY manufacturer
            ORDER BY aircraft_count DESC
        """
        
        manufacturer_df = get_dataframe_from_query(query_manufacturer)
        
        if not manufacturer_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = plot_bar_chart(manufacturer_df.head(10), 'manufacturer', 'aircraft_count',
                                    'Top 10 Aircraft Manufacturers')
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = plot_bar_chart(manufacturer_df.head(10), 'manufacturer', 'total_flights',
                                    'Total Flights by Manufacturer')
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("Route Analysis")
        
        # Query 4: Top destination airports
        st.subheader("4. Top Destination Airports")
        query4_df = get_dataframe_from_query(FlightQueries.QUERY_4)
        
        if not query4_df.empty:
            fig = plot_bar_chart(query4_df, 'airport_name', 'arriving_flights',
                                'Top Destination Airports')
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(query4_df, use_container_width=True)
        
        # Query 10: City pairs with multiple aircraft models
        st.subheader("10. City Pairs with >2 Aircraft Models")
        query10_df = get_dataframe_from_query(FlightQueries.QUERY_10)
        
        if not query10_df.empty:
            fig = px.scatter(query10_df, x='total_flights', y='unique_aircraft_models',
                            size='unique_aircraft_models', hover_data=['origin_city', 'destination_city'],
                            title='Route Diversity Analysis')
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(query10_df, use_container_width=True)
        
        # Busiest routes
        st.subheader("Busiest Routes")
        query_busy_routes = """
            SELECT 
                o.name as origin_airport,
                d.name as destination_airport,
                COUNT(*) as flight_count,
                AVG(CASE WHEN f.status = 'Delayed' THEN 1 ELSE 0 END) * 100 as delay_percentage
            FROM flights f
            JOIN airport o ON f.origin_iata = o.iata_code
            JOIN airport d ON f.destination_iata = d.iata_code
            GROUP BY f.origin_iata, f.destination_iata, o.name, d.name
            HAVING flight_count > 5
            ORDER BY flight_count DESC
            LIMIT 20
        """
        
        busy_routes_df = get_dataframe_from_query(query_busy_routes)
        
        if not busy_routes_df.empty:
            # Create route labels
            busy_routes_df['route'] = busy_routes_df['origin_airport'] + ' → ' + busy_routes_df['destination_airport']
            
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add flight count bars
            fig.add_trace(
                go.Bar(
                    x=busy_routes_df['route'],
                    y=busy_routes_df['flight_count'],
                    name="Flight Count",
                    marker_color='blue'
                ),
                secondary_y=False
            )
            
            # Add delay percentage line
            fig.add_trace(
                go.Scatter(
                    x=busy_routes_df['route'],
                    y=busy_routes_df['delay_percentage'],
                    name="Delay %",
                    marker_color='red',
                    mode='lines+markers'
                ),
                secondary_y=True
            )
            
            fig.update_layout(
                title="Busiest Routes with Delay Percentage",
                xaxis_tickangle=-45,
                height=500,
                showlegend=True
            )
            
            fig.update_yaxes(title_text="Flight Count", secondary_y=False)
            fig.update_yaxes(title_text="Delay %", secondary_y=True)
            
            st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.subheader("Project SQL Queries")
        
        # List all project queries
        queries = {
            "Query 1: Flights per Aircraft Model": FlightQueries.QUERY_1,
            "Query 2: Aircraft with >5 Flights": FlightQueries.QUERY_2,
            "Query 3: Airports with >5 Outbound Flights": FlightQueries.QUERY_3,
            "Query 4: Top 3 Destination Airports": FlightQueries.QUERY_4,
            "Query 5: Domestic/International Flights": FlightQueries.QUERY_5,
            "Query 6: Recent Arrivals at DEL": FlightQueries.QUERY_6,
            "Query 7: Airports with No Arrivals": FlightQueries.QUERY_7,
            "Query 8: Airline Performance by Status": FlightQueries.QUERY_8,
            "Query 9: Cancelled Flights": FlightQueries.QUERY_9,
            "Query 10: City Pairs with >2 Aircraft Models": FlightQueries.QUERY_10,
            "Query 11: Airport Delay Percentage": FlightQueries.QUERY_11
        }
        
        selected_query_name = st.selectbox("Select Query to Execute", list(queries.keys()))
        
        if st.button("Run Selected Query"):
            with st.spinner("Executing query..."):
                result_df = get_dataframe_from_query(queries[selected_query_name])
                
                if not result_df.empty:
                    st.success(f"Query executed successfully. Returned {len(result_df)} rows.")
                    
                    # Display results
                    st.dataframe(result_df, use_container_width=True)
                    
                    # Show some basic stats
                    st.subheader("Query Statistics")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Number of Rows", len(result_df))
                    with col2:
                        st.metric("Number of Columns", len(result_df.columns))
                    
                    # Show query
                    with st.expander("View SQL Query"):
                        st.code(queries[selected_query_name], language="sql")
                else:
                    st.info("Query returned no results")
        
        # Batch execute all queries
        st.subheader("Batch Execute All Queries")
        if st.button("Run All Queries (This may take a moment)"):
            results = {}
            
            for name, query in queries.items():
                with st.spinner(f"Running {name}..."):
                    df = get_dataframe_from_query(query)
                    results[name] = df
            
            # Display summary
            st.subheader("Query Results Summary")
            summary_data = []
            for name, df in results.items():
                summary_data.append({
                    "Query": name.replace("Query ", "").split(":")[0],
                    "Description": name.split(":")[1].strip(),
                    "Rows Returned": len(df),
                    "Columns": len(df.columns) if not df.empty else 0
                })
            
            summary_df = pd.DataFrame(summary_data)
            st.dataframe(summary_df, use_container_width=True)

if __name__ == "__main__":
    analytics_page()