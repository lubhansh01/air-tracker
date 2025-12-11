"""
Minimal working database
"""

import sqlite3
import pandas as pd

class FlightDatabase:
    def __init__(self, db_name='flight_analytics.db'):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.create_tables()
    
    def create_tables(self):
        """Create only essential tables"""
        cursor = self.conn.cursor()
        
        # Simple airport table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS airport (
            airport_id INTEGER PRIMARY KEY AUTOINCREMENT,
            iata_code TEXT UNIQUE,
            name TEXT,
            city TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL
        )
        ''')
        
        # Simple flights table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS flights (
            flight_id TEXT PRIMARY KEY,
            flight_number TEXT,
            airline_name TEXT,
            origin_iata TEXT,
            destination_iata TEXT,
            scheduled_departure TEXT,
            status TEXT
        )
        ''')
        
        # Simple delays table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS airport_delays (
            airport_iata TEXT,
            total_flights INTEGER,
            delayed_flights INTEGER,
            avg_delay_min REAL
        )
        ''')
        
        self.conn.commit()
        print("✅ Database ready")
    
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
    
    def close(self):
        """Close connection"""
        self.conn.close()