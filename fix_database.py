"""
Script to fix database issues
"""

import os
import sqlite3

def fix_database():
    """Fix database schema issues"""
    print("🔧 Fixing database...")
    
    try:
        # Delete existing database file
        if os.path.exists('flight_analytics.db'):
            os.remove('flight_analytics.db')
            print("✅ Old database removed")
        
        # Create new database with correct schema
        conn = sqlite3.connect('flight_analytics.db')
        cursor = conn.cursor()
        
        # Create airport table WITHOUT region first
        cursor.execute('''
        CREATE TABLE airport (
            airport_id INTEGER PRIMARY KEY AUTOINCREMENT,
            icao_code TEXT,
            iata_code TEXT UNIQUE,
            name TEXT,
            city TEXT,
            country TEXT,
            continent TEXT,
            latitude REAL,
            longitude REAL,
            timezone TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Create other tables
        cursor.execute('''
        CREATE TABLE flights (
            flight_id TEXT PRIMARY KEY,
            flight_number TEXT,
            aircraft_registration TEXT,
            origin_iata TEXT,
            destination_iata TEXT,
            scheduled_departure TEXT,
            actual_departure TEXT,
            scheduled_arrival TEXT,
            actual_arrival TEXT,
            status TEXT,
            airline_code TEXT,
            airline_name TEXT,
            flight_date DATE,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE airport_delays (
            delay_id INTEGER PRIMARY KEY AUTOINCREMENT,
            airport_iata TEXT,
            delay_date DATE,
            total_flights INTEGER,
            delayed_flights INTEGER,
            avg_delay_min REAL,
            median_delay_min REAL,
            canceled_flights INTEGER,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        conn.commit()
        conn.close()
        
        print("✅ New database created with correct schema")
        print("\nNow run:")
        print("   streamlit run dashboard.py")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    fix_database()