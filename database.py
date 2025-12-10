"""
Database for 15 airports
"""

import sqlite3
import pandas as pd

class FlightDatabase:
    def __init__(self, db_name='flight_analytics.db'):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.create_tables()
    
    def create_tables(self):
        """Create tables for 15 airports"""
        cursor = self.conn.cursor()
        
        # Airport table with region
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS airport (
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
            region TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Create index for region-based queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_airport_region ON airport(region)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_airport_iata ON airport(iata_code)')
        
        # Flights table
        cursor.execute('''
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
            airline_name TEXT,
            flight_date DATE,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Create indexes for flight queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_flights_origin ON flights(origin_iata)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_flights_dest ON flights(destination_iata)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_flights_date ON flights(flight_date)')
        
        # Airport delays table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS airport_delays (
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
        
        # Create index for delay queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_delays_airport ON airport_delays(airport_iata)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_delays_date ON airport_delays(delay_date)')
        
        self.conn.commit()
    
    def execute_query(self, query, params=None, return_df=False):
        """Execute SQL query"""
        try:
            if return_df:
                return pd.read_sql_query(query, self.conn, params=params)
            else:
                cursor = self.conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                if query.strip().upper().startswith('SELECT'):
                    return cursor.fetchall()
                else:
                    self.conn.commit()
                    return cursor.rowcount
        except Exception as e:
            print(f"Query error: {e}")
            return None
    
    def get_airport_stats(self):
        """Get statistics for all airports"""
        query = '''
        SELECT 
            a.region,
            COUNT(DISTINCT a.iata_code) as airports,
            COUNT(DISTINCT f.flight_id) as flights,
            COALESCE(ROUND(AVG(d.avg_delay_min), 1), 0) as avg_delay
        FROM airport a
        LEFT JOIN flights f ON a.iata_code = f.origin_iata 
            AND DATE(f.flight_date) = DATE('now')
        LEFT JOIN airport_delays d ON a.iata_code = d.airport_iata 
            AND d.delay_date = DATE('now')
        GROUP BY a.region
        ORDER BY airports DESC
        '''
        return self.execute_query(query, return_df=True)
    
    def close(self):
        """Close connection"""
        self.conn.close()