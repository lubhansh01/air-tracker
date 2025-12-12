import sqlite3
import os
from datetime import datetime

class DatabaseManager:
    def __init__(self):
        # Use SQLite instead of MySQL
        self.db_path = 'data/flight_analytics.db'
        self.connection = None
    
    def connect(self):
        """Connect to SQLite database"""
        try:
            # Create data directory if it doesn't exist
            os.makedirs('data', exist_ok=True)
            
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row  # Return rows as dictionaries
            print(f"✅ Connected to SQLite database: {self.db_path}")
            return True
        except Exception as e:
            print(f"❌ Error connecting to SQLite: {e}")
            return False
    
    def create_tables(self):
        """Create database tables"""
        if not self.connection:
            print("No database connection")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Create airport table
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
            print("✅ Created airport table")
            
            # Create aircraft table
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
            print("✅ Created aircraft table")
            
            # Create flights table
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
                    flight_date TEXT,
                    FOREIGN KEY (aircraft_registration) REFERENCES aircraft(registration) ON DELETE SET NULL,
                    FOREIGN KEY (origin_iata) REFERENCES airport(iata_code) ON DELETE SET NULL,
                    FOREIGN KEY (destination_iata) REFERENCES airport(iata_code) ON DELETE SET NULL
                )
            """)
            print("✅ Created flights table")
            
            # Create airport_delays table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS airport_delays (
                    delay_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    airport_iata TEXT,
                    delay_date TEXT,
                    total_flights INTEGER,
                    delayed_flights INTEGER,
                    avg_delay_min INTEGER,
                    median_delay_min INTEGER,
                    canceled_flights INTEGER,
                    FOREIGN KEY (airport_iata) REFERENCES airport(iata_code) ON DELETE CASCADE
                )
            """)
            print("✅ Created airport_delays table")
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_airport_iata ON airport(iata_code)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_airport_country ON airport(country)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_aircraft_registration ON aircraft(registration)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_flights_origin ON flights(origin_iata)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_flights_destination ON flights(destination_iata)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_flights_status ON flights(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_flights_date ON flights(flight_date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_flights_airline ON flights(airline_code)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_delays_airport_date ON airport_delays(airport_iata, delay_date)")
            
            self.connection.commit()
            print("✅ All tables and indexes created successfully")
            return True
            
        except Exception as e:
            print(f"❌ Error creating tables: {e}")
            return False
    
    def get_connection(self):
        """Get database connection"""
        return self.connection
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("Database connection closed")
    
    def execute_query(self, query, params=None, fetch=False):
        """Execute SQL query"""
        try:
            cursor = self.connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if fetch:
                # For SQLite, we need to fetch column names separately
                columns = [description[0] for description in cursor.description]
                result = cursor.fetchall()
                # Convert to list of dictionaries
                result_dicts = []
                for row in result:
                    result_dicts.append(dict(zip(columns, row)))
                cursor.close()
                return result_dicts
            else:
                self.connection.commit()
                cursor.close()
                return True
                
        except Exception as e:
            print(f"Error executing query: {e}")
            print(f"Query: {query}")
            return None