"""
Database management for Flight Analytics
"""

import sqlite3
import pandas as pd
from datetime import datetime

class FlightDatabase:
    def __init__(self, db_name='flight_analytics.db'):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.create_tables()
    
    def create_tables(self):
        """Create all necessary tables if they don't exist"""
        cursor = self.conn.cursor()
        
        # Airport table
        cursor.execute('''
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
            timezone TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Aircraft table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS aircraft (
            aircraft_id INTEGER PRIMARY KEY AUTOINCREMENT,
            registration TEXT UNIQUE,
            model TEXT,
            manufacturer TEXT,
            icao_type_code TEXT,
            owner TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
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
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (origin_iata) REFERENCES airport(iata_code),
            FOREIGN KEY (destination_iata) REFERENCES airport(iata_code)
        )
        ''')
        
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
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (airport_iata) REFERENCES airport(iata_code)
        )
        ''')
        
        self.conn.commit()
    
    def execute_query(self, query, params=None, return_df=False):
        """Execute SQL query and optionally return DataFrame"""
        try:
            if return_df:
                return pd.read_sql_query(query, self.conn, params=params)
            else:
                cursor = self.conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # For SELECT queries, fetch results
                if query.strip().upper().startswith('SELECT'):
                    return cursor.fetchall()
                else:
                    self.conn.commit()
                    return cursor.rowcount
        except Exception as e:
            print(f"Query error: {e}")
            return None
    
    def close(self):
        """Close database connection"""
        self.conn.close()