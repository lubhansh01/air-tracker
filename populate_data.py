#!/usr/bin/env python3
"""
Data Population Script
Run this script to collect data from AeroDataBox API and populate the database
"""

import os
import sys
from dotenv import load_dotenv
import time

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_setup import DatabaseManager
from api.data_collector import DataCollector

def populate_data():
    """Collect data from API and populate database"""
    print("🚀 Starting data collection from AeroDataBox API...")
    
    # Load environment variables
    load_dotenv()
    
    # Check for API key
    api_key = os.getenv("AERODATABOX_API_KEY")
    if not api_key or api_key == "your_api_key_here":
        print("❌ Please set your AeroDataBox API key in .env file")
        print("   Get your API key from: https://rapidapi.com/aedbx-aedbx/api/aerodatabox")
        return
    
    # Initialize database connection
    db_manager = DatabaseManager()
    if not db_manager.connect():
        print("❌ Failed to connect to database")
        return
    
    print("✅ Connected to database")
    
    # Initialize data collector
    collector = DataCollector(db_manager.get_connection())
    
    try:
        # Step 1: Collect airport data
        print("\n📡 Step 1: Collecting airport data...")
        airports_df = collector.collect_airport_data()
        if not airports_df.empty:
            collector.save_to_database(airports_df, 'airport')
            print(f"✅ Collected data for {len(airports_df)} airports")
        else:
            print("❌ Failed to collect airport data")
            return
        
        time.sleep(2)
        
        # Step 2: Collect flights data
        print("\n📡 Step 2: Collecting flights data...")
        flights_df, aircraft_registrations = collector.collect_flights_data(days_back=3)
        if not flights_df.empty:
            collector.save_to_database(flights_df, 'flights')
            print(f"✅ Collected {len(flights_df)} flights")
            print(f"✅ Found {len(aircraft_registrations)} unique aircraft registrations")
        else:
            print("⚠️ No flights data collected, using sample data")
            # You could add sample data here
        
        time.sleep(2)
        
        # Step 3: Collect aircraft data
        print("\n📡 Step 3: Collecting aircraft data...")
        if aircraft_registrations:
            aircraft_df = collector.collect_aircraft_data(aircraft_registrations)
            if not aircraft_df.empty:
                collector.save_to_database(aircraft_df, 'aircraft')
                print(f"✅ Collected data for {len(aircraft_df)} aircraft")
        
        time.sleep(2)
        
        # Step 4: Collect delay data
        print("\n📡 Step 4: Collecting delay data...")
        delays_df = collector.collect_delay_data()
        if not delays_df.empty:
            collector.save_to_database(delays_df, 'airport_delays')
            print(f"✅ Collected delay data for {len(delays_df)} airports")
        
        print("\n🎉 Data collection completed successfully!")
        print("\n📊 Data Summary:")
        print(f"   Airports: {len(airports_df)}")
        print(f"   Flights: {len(flights_df)}")
        print(f"   Aircraft: {len(aircraft_df) if 'aircraft_df' in locals() else 0}")
        print(f"   Delay Records: {len(delays_df)}")
        
        # Display sample data
        print("\n📋 Sample Data Preview:")
        if not flights_df.empty:
            print("\nSample Flights:")
            print(flights_df[['flight_number', 'origin_iata', 'destination_iata', 'status']].head())
        
    except Exception as e:
        print(f"❌ Error during data collection: {e}")
    finally:
        db_manager.close()
        print("\n🔒 Database connection closed")

if __name__ == "__main__":
    populate_data()