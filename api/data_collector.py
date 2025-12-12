import pandas as pd
from datetime import datetime, timedelta
import time
from api.aerodatabox_api import AeroDataBoxAPI
from api.config import Config
import mysql.connector
from mysql.connector import Error

class DataCollector:
    def __init__(self, db_connection=None):
        self.api = AeroDataBoxAPI()
        self.db_connection = db_connection
        self.airport_codes = Config.AIRPORT_CODES
        
    def collect_airport_data(self):
        """Collect airport information"""
        airports_data = []
        
        for iata_code in self.airport_codes:
            print(f"Fetching data for airport: {iata_code}")
            data = self.api.get_airport_info(iata_code)
            
            if data:
                airport_info = {
                    'icao_code': data.get('icao'),
                    'iata_code': data.get('iata'),
                    'name': data.get('name'),
                    'city': data.get('municipalityName', ''),
                    'country': data.get('country', {}).get('name', ''),
                    'continent': 'Asia' if data.get('country', {}).get('code') in ['IN'] else 'North America' if data.get('country', {}).get('code') in ['US'] else 'Europe',
                    'latitude': data.get('location', {}).get('lat'),
                    'longitude': data.get('location', {}).get('lon'),
                    'timezone': data.get('timeZone')
                }
                airports_data.append(airport_info)
            
            time.sleep(0.5)  # Rate limiting
        
        return pd.DataFrame(airports_data)
    
    def collect_flights_data(self, days_back=7):
        """Collect flights data for all airports"""
        all_flights = []
        aircraft_registrations = set()
        
        for iata_code in self.airport_codes:
            print(f"Fetching flights for airport: {iata_code}")
            flights = self.api.get_flights_for_period(iata_code, days_back)
            
            for flight in flights:
                flight_info = self._extract_flight_info(flight)
                if flight_info:
                    all_flights.append(flight_info)
                    
                    # Collect aircraft registration for later lookup
                    if flight_info.get('aircraft_registration'):
                        aircraft_registrations.add(flight_info['aircraft_registration'])
            
            time.sleep(1)  # Rate limiting
        
        return pd.DataFrame(all_flights), list(aircraft_registrations)
    
    def collect_aircraft_data(self, registrations):
        """Collect aircraft information"""
        aircraft_data = []
        
        for reg in registrations[:50]:  # Limit to 50 aircraft
            print(f"Fetching aircraft info: {reg}")
            data = self.api.get_aircraft_info(reg)
            
            if data:
                aircraft_info = {
                    'registration': reg,
                    'model': data.get('model'),
                    'manufacturer': data.get('manufacturer'),
                    'icao_type_code': data.get('icaoCode'),
                    'owner': data.get('owner')
                }
                aircraft_data.append(aircraft_info)
            
            time.sleep(0.5)
        
        return pd.DataFrame(aircraft_data)
    
    def collect_delay_data(self):
        """Collect airport delay statistics"""
        delay_data = []
        today = datetime.now().strftime('%Y-%m-%d')
        
        for iata_code in self.airport_codes:
            print(f"Fetching delay data for: {iata_code}")
            data = self.api.get_airport_delays(iata_code)
            
            if data and 'statistics' in data:
                stats = data['statistics']
                delay_info = {
                    'airport_iata': iata_code,
                    'delay_date': today,
                    'total_flights': stats.get('flights', {}).get('total', 0),
                    'delayed_flights': stats.get('flights', {}).get('delayed', 0),
                    'avg_delay_min': stats.get('minutesDelayed', {}).get('avg', 0),
                    'median_delay_min': stats.get('minutesDelayed', {}).get('median', 0),
                    'canceled_flights': stats.get('flights', {}).get('cancelled', 0)
                }
                delay_data.append(delay_info)
            
            time.sleep(0.5)
        
        return pd.DataFrame(delay_data)
    
    def _extract_flight_info(self, flight):
        """Extract relevant flight information from API response"""
        try:
            flight_id = flight.get('number', {}).get('default', '')
            if not flight_id:
                return None
            
            return {
                'flight_id': f"{flight_id}_{flight.get('movement', {}).get('scheduledTime', {}).get('utc', '')}",
                'flight_number': flight_id,
                'aircraft_registration': flight.get('aircraft', {}).get('reg', ''),
                'origin_iata': flight.get('departure', {}).get('airport', {}).get('icao', '')[:3] if flight.get('departure', {}).get('airport') else '',
                'destination_iata': flight.get('arrival', {}).get('airport', {}).get('icao', '')[:3] if flight.get('arrival', {}).get('airport') else '',
                'scheduled_departure': flight.get('movement', {}).get('scheduledTime', {}).get('utc', ''),
                'actual_departure': flight.get('movement', {}).get('actualTime', {}).get('utc', '') if flight.get('movement', {}).get('actualTime') else None,
                'scheduled_arrival': flight.get('arrival', {}).get('scheduledTime', {}).get('utc', '') if flight.get('arrival') else None,
                'actual_arrival': flight.get('arrival', {}).get('actualTime', {}).get('utc', '') if flight.get('arrival', {}).get('actualTime') else None,
                'status': self._determine_status(flight),
                'airline_code': flight.get('airline', {}).get('code', {}).get('iata', ''),
                'direction': flight.get('direction', ''),
                'date': flight.get('date', '')
            }
        except Exception as e:
            print(f"Error extracting flight info: {e}")
            return None
    
    def _determine_status(self, flight):
        """Determine flight status"""
        status = flight.get('status', '')
        
        if status == 'Canceled':
            return 'Cancelled'
        elif status == 'Delayed':
            return 'Delayed'
        elif status in ['Active', 'Landed', 'Diverted']:
            return 'Completed'
        else:
            return 'Scheduled'
    
    def save_to_database(self, df, table_name):
        """Save DataFrame to database"""
        if self.db_connection is None:
            print("No database connection available")
            return False
        
        try:
            cursor = self.db_connection.cursor()
            
            for _, row in df.iterrows():
                placeholders = ', '.join(['%s'] * len(row))
                columns = ', '.join(row.index)
                sql = f"INSERT IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
                cursor.execute(sql, tuple(row))
            
            self.db_connection.commit()
            print(f"Saved {len(df)} records to {table_name}")
            return True
            
        except Error as e:
            print(f"Error saving to database: {e}")
            return False