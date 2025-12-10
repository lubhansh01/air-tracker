import requests
import json
from datetime import datetime, timedelta
import time
from config import HEADERS, API_HOST, AIRPORT_CODES

class AerodataboxAPI:
    def __init__(self):
        self.base_url = f"https://{API_HOST}"
    
    def get_airport_info(self, iata_code):
        """Get airport information by IATA code"""
        url = f"{self.base_url}/airports/icao/{iata_code}"
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching airport info for {iata_code}: {e}")
            return None
    
    def get_flights_by_airport(self, iata_code, direction="arrivals", hours_from_now=12):
        """Get flights for a specific airport"""
        url = f"{self.base_url}/flights/airports/icao/{iata_code}/{direction}"
        params = {
            'withLeg': 'true',
            'direction': direction,
            'withCancelled': 'true',
            'withCodeshared': 'true',
            'withCargo': 'true',
            'withPrivate': 'true',
            'withLocation': 'false'
        }
        
        try:
            response = requests.get(url, headers=HEADERS, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching flights for {iata_code}: {e}")
            return None
    
    def get_aircraft_info(self, registration):
        """Get aircraft information by registration"""
        url = f"{self.base_url}/aircrafts/reg/{registration}"
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching aircraft info for {registration}: {e}")
            return None
    
    def get_airport_delays(self, iata_code):
        """Get delay statistics for an airport"""
        url = f"{self.base_url}/airports/icao/{iata_code}/delays"
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching delays for {iata_code}: {e}")
            return None

class DataFetcher:
    def __init__(self, db):
        self.api = AerodataboxAPI()
        self.db = db
    
    def fetch_all_data(self):
        """Fetch all data for selected airports"""
        print("Starting data fetch...")
        
        # Fetch airport data
        for code in AIRPORT_CODES:
            self.fetch_airport_data(code)
            time.sleep(1)  # Rate limiting
        
        # Fetch flights
        for code in AIRPORT_CODES:
            self.fetch_flight_data(code)
            time.sleep(2)
        
        # Fetch delays
        for code in AIRPORT_CODES[:5]:  # Limit to 5 airports for delays
            self.fetch_delay_data(code)
            time.sleep(1)
        
        print("Data fetch completed!")
    
    def fetch_airport_data(self, iata_code):
        """Fetch and store airport information"""
        data = self.api.get_airport_info(iata_code)
        if data:
            query = '''
            INSERT OR REPLACE INTO airport 
            (icao_code, iata_code, name, city, country, continent, latitude, longitude, timezone)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            params = (
                data.get('icao'),
                data.get('iata'),
                data.get('name'),
                data.get('location', {}).get('city', {}).get('name', ''),
                data.get('country', {}).get('name', ''),
                data.get('continent'),
                data.get('location', {}).get('latitude'),
                data.get('location', {}).get('longitude'),
                data.get('timeZone')
            )
            self.db.execute_query(query, params)
    
    def fetch_flight_data(self, iata_code):
        """Fetch and store flight data"""
        for direction in ['arrivals', 'departures']:
            data = self.api.get_flights_by_airport(iata_code, direction)
            if data and 'data' in data:
                for flight in data['data']:
                    self.process_flight(flight, direction)
    
    def process_flight(self, flight_data, direction):
        """Process individual flight data"""
        try:
            flight_id = flight_data.get('number')
            if not flight_id:
                return
            
            # Determine origin and destination based on direction
            if direction == 'arrivals':
                origin = flight_data.get('departure', {}).get('airport', {}).get('icao')
                destination = flight_data.get('arrival', {}).get('airport', {}).get('icao')
            else:
                origin = flight_data.get('departure', {}).get('airport', {}).get('icao')
                destination = flight_data.get('arrival', {}).get('airport', {}).get('icao')
            
            # Get aircraft registration
            aircraft = flight_data.get('aircraft', {}).get('reg')
            
            query = '''
            INSERT OR REPLACE INTO flights 
            (flight_id, flight_number, aircraft_registration, origin_iata, destination_iata,
             scheduled_departure, actual_departure, scheduled_arrival, actual_arrival,
             status, airline_code, airline_name, flight_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE('now'))
            '''
            
            params = (
                flight_id,
                flight_data.get('number'),
                aircraft,
                origin,
                destination,
                flight_data.get('departure', {}).get('scheduledTime', {}).get('local'),
                flight_data.get('departure', {}).get('actualTime', {}).get('local'),
                flight_data.get('arrival', {}).get('scheduledTime', {}).get('local'),
                flight_data.get('arrival', {}).get('actualTime', {}).get('local'),
                flight_data.get('status'),
                flight_data.get('airline', {}).get('code'),
                flight_data.get('airline', {}).get('name')
            )
            
            self.db.execute_query(query, params)
            
            # Fetch aircraft data if available
            if aircraft:
                self.fetch_aircraft_data(aircraft)
                
        except Exception as e:
            print(f"Error processing flight: {e}")
    
    def fetch_aircraft_data(self, registration):
        """Fetch and store aircraft information"""
        data = self.api.get_aircraft_info(registration)
        if data:
            query = '''
            INSERT OR REPLACE INTO aircraft 
            (registration, model, manufacturer, icao_type_code, owner)
            VALUES (?, ?, ?, ?, ?)
            '''
            params = (
                registration,
                data.get('model'),
                data.get('manufacturer'),
                data.get('icaoTypeCode'),
                data.get('owner')
            )
            self.db.execute_query(query, params)
    
    def fetch_delay_data(self, iata_code):
        """Fetch and store delay statistics"""
        data = self.api.get_airport_delays(iata_code)
        if data:
            query = '''
            INSERT OR REPLACE INTO airport_delays 
            (airport_iata, delay_date, total_flights, delayed_flights, 
             avg_delay_min, median_delay_min, canceled_flights)
            VALUES (?, DATE('now'), ?, ?, ?, ?, ?)
            '''
            params = (
                iata_code,
                data.get('totalFlights', 0),
                data.get('delayedFlights', 0),
                data.get('averageDelayMinutes', 0),
                data.get('medianDelayMinutes', 0),
                data.get('canceledFlights', 0)
            )
            self.db.execute_query(query, params)