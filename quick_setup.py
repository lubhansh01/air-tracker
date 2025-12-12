#!/usr/bin/env python3
"""
Quick Setup Script for Flight Analytics Project
"""

import os
import sys
import sqlite3
import subprocess

def setup_project():
    print("🚀 Quick Setup for Flight Analytics Project")
    print("=" * 50)
    
    # Create necessary directories
    print("\n📁 Creating project structure...")
    directories = ['data', 'api', 'database', 'app']
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"  Created: {directory}/")
    
    # Create .env file if it doesn't exist
    print("\n🔧 Setting up environment...")
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write("""# AeroDataBox API Configuration
AERODATABOX_API_KEY=your_api_key_here

# Note: Using SQLite database (no configuration needed)
""")
        print("  Created .env file")
    
    # Check if requirements.txt exists
    if not os.path.exists('requirements.txt'):
        print("\n❌ requirements.txt not found")
        return False
    
    # Install dependencies
    print("\n📦 Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("  ✅ Dependencies installed")
    except:
        print("  ⚠️ Could not install dependencies automatically")
        print("  Please run: pip install -r requirements.txt")
    
    # Setup SQLite database
    print("\n🗄️ Setting up SQLite database...")
    try:
        import sqlite3
        conn = sqlite3.connect('data/flight_analytics.db')
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS airport (
                airport_id INTEGER PRIMARY KEY AUTOINCREMENT,
                icao_code TEXT UNIQUE,
                iata_code TEXT UNIQUE,
                name TEXT,
                city TEXT,
                country TEXT,
                continent TEXT,
                latitude REAL,
                longitude REAL,
                timezone TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS aircraft (
                aircraft_id INTEGER PRIMARY KEY AUTOINCREMENT,
                registration TEXT UNIQUE,
                model TEXT,
                manufacturer TEXT,
                icao_type_code TEXT,
                owner TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS flights (
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
                direction TEXT,
                flight_date TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS airport_delays (
                delay_id INTEGER PRIMARY KEY AUTOINCREMENT,
                airport_iata TEXT,
                delay_date TEXT,
                total_flights INTEGER,
                delayed_flights INTEGER,
                avg_delay_min INTEGER,
                median_delay_min INTEGER,
                canceled_flights INTEGER
            )
        """)
        
        conn.commit()
        conn.close()
        print("  ✅ Database created: data/flight_analytics.db")
        
    except Exception as e:
        print(f"  ❌ Database setup failed: {e}")
        return False
    
    print("\n" + "=" * 50)
    print("🎉 Setup completed successfully!")
    print("\n📝 Next steps:")
    print("1. Get your API key from: https://rapidapi.com/aedbx-aedbx/api/aerodatabox")
    print("2. Add your API key to the .env file")
    print("3. Run: python populate_data.py (to collect data)")
    print("4. Run: streamlit run main.py (to start the app)")
    print("\n✅ Your project is ready!")
    
    return True

if __name__ == "__main__":
    setup_project()