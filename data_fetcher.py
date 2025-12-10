"""
Intelligent data fetcher for Flight Analytics
"""

import time
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from aerodatabox_client import AeroDataBoxClient
from database import FlightDatabase
from config import AIRPORT_CODES, FETCH_STRATEGY

class SmartDataFetcher:
    """Fetches data intelligently and stores in database"""
    
    def __init__(self, db: FlightDatabase):
        self.client = AeroDataBoxClient()
        self.db = db
        self.last_fetch = None
    
    def fetch_dashboard_data(self) -> Dict:
        """Fetch all data needed for dashboard overview"""
        print("🚀 Fetching dashboard data...")
        start_time = time.time()
        
        # Limit to 4 airports for dashboard
        dashboard_airports = AIRPORT_CODES[:4]
        
        results = {
            'airports': {},
            'flights': {},
            'delays': {},
            'weather': {}
        }
        
        # 1. Fetch airport basic info (parallel)
        print("🌎 Fetching airport info...")
        airports_data = self.client.get_multiple_airports(dashboard_airports)
        results['airports'] = airports_data
        
        # 2. Fetch flights for airports
        print("✈️ Fetching flight schedules...")
        flight_airports = dashboard_airports[:2]
        flights_data = self.client.get_multiple_flights(flight_airports)
        results['flights'] = flights_data
        
        # 3. Fetch delays for first airport only
        print("⏱️ Fetching delay statistics...")
        if dashboard_airports:
            delays = self.client.get_airport_delays(dashboard_airports[0])
            results['delays'] = {dashboard_airports[0]: delays}
        
        # 4. Fetch weather for first airport
        print("🌤️ Fetching weather...")
        if dashboard_airports:
            weather = self.client.get_airport_weather(dashboard_airports[0])
            results['weather'] = {dashboard_airports[0]: weather}
        
        # 5. Store data in database
        self._store_fetched_data(results)
        
        elapsed = time.time() - start_time
        self.last_fetch = datetime.now()
        
        flights_count = sum(len(f.get('data', [])) if f else 0 
                          for f in flights_data.values())
        
        print(f"✅ Dashboard data fetched in {elapsed:.1f} seconds")
        
        return {
            'success': True,
            'time': elapsed,
            'airports_fetched': len([d for d in airports_data.values() if d]),
            'flights_fetched': flights_count,
            'api_stats': self.client.get_stats()
        }
    
    def fetch_airport_details(self, airport_code: str) -> Dict:
        """Fetch detailed information for a specific airport"""
        details = {
            'basic_info': None,
            'local_time': None,
            'weather': None,
            'current_flights': None,
            'delays': None
        }
        
        # Fetch in parallel for speed
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                'basic_info': executor.submit(
                    self.client.get_airport_info, airport_code
                ),
                'local_time': executor.submit(
                    self.client.get_airport_time, airport_code
                ),
                'weather': executor.submit(
                    self.client.get_airport_weather, airport_code
                ),
                'current_flights': executor.submit(
                    self.client.get_airport_flights, airport_code, 'departures'
                )
            }
            
            # Delays only if really needed
            if airport_code in AIRPORT_CODES[:3]:
                futures['delays'] = executor.submit(
                    self.client.get_airport_delays, airport_code
                )
            
            for key, future in futures.items():
                try:
                    details[key] = future.result(timeout=FETCH_STRATEGY['timeout'])
                except Exception as e:
                    print(f"Error fetching {key} for {airport_code}: {e}")
        
        return details
    
    def search_flights(self, query: str) -> List[Dict]:
        """Search flights by various criteria"""
        results = []
        
        # Auto-detect search type
        if '-' in query and len(query.split('-')) == 2:
            # Route search (e.g., DEL-BOM)
            origin, destination = query.split('-')
            flights = self.client.get_airport_flights(origin, 'departures')
            if flights and 'data' in flights:
                for flight in flights['data']:
                    arr_airport = flight.get('arrival', {}).get('airport', {}).get('iata', '')
                    if arr_airport == destination.upper():
                        results.append(flight)
        
        elif len(query) in [6, 7] and query[:2].isalpha():
            # Flight number search
            flight_data = self.client.get_flight_status(query)
            if flight_data:
                results.append(flight_data)
        
        elif len(query) in [3, 4] and query.isalpha():
            # Airport code search
            flights = self.client.get_airport_flights(query)
            if flights and 'data' in flights:
                results.extend(flights['data'][:20])
        
        else:
            # Text search - get flights from major airports
            for airport in AIRPORT_CODES[:2]:
                flights = self.client.get_airport_flights(airport, 'departures')
                if flights and 'data' in flights:
                    for flight in flights['data'][:10]:
                        airline_name = flight.get('airline', {}).get('name', '').lower()
                        flight_num = flight.get('number', '').lower()
                        if query.lower() in airline_name or query.lower() in flight_num:
                            results.append(flight)
        
        return results
    
    def get_aircraft_info(self, registration: str) -> Optional[Dict]:
        """Get detailed aircraft information"""
        return self.client.get_aircraft_info(registration)
    
    def _store_fetched_data(self, data: Dict):
        """Store fetched data in database"""
        try:
            # Store airport data
            for airport_code, airport_data in data.get('airports', {}).items():
                if airport_data:
                    self._store_airport(airport_code, airport_data)
            
            # Store flight data
            for airport_code, flights_data in data.get('flights', {}).items():
                if flights_data and 'data' in flights_data:
                    for flight in flights_data['data'][:10]:
                        self._store_flight(flight)
            
            # Store delay data
            for airport_code, delay_data in data.get('delays', {}).items():
                if delay_data:
                    self._store_delays(airport_code, delay_data)
            
        except Exception as e:
            print(f"Error storing data: {e}")
    
    def _store_airport(self, code: str, data: Dict):
        """Store airport in database"""
        try:
            query = '''
            INSERT OR REPLACE INTO airport 
            (icao_code, iata_code, name, city, country, continent, 
             latitude, longitude, timezone)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            params = (
                data.get('icao', code),
                data.get('iata', code),
                data.get('name', ''),
                data.get('municipalityName', ''),
                data.get('country', {}).get('name', ''),
                data.get('continent', ''),
                data.get('location', {}).get('lat', 0),
                data.get('location', {}).get('lon', 0),
                data.get('timeZone', '')
            )
            
            self.db.execute_query(query, params)
        except Exception as e:
            print(f"Error storing airport {code}: {e}")
    
    def _store_flight(self, flight_data: Dict):
        """Store flight in database"""
        try:
            flight_number = flight_data.get('number')
            if not flight_number:
                return
            
            query = '''
            INSERT OR REPLACE INTO flights 
            (flight_id, flight_number, aircraft_registration, origin_iata, 
             destination_iata, scheduled_departure, actual_departure, 
             scheduled_arrival, actual_arrival, status, airline_code, 
             airline_name, flight_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE('now'))
            '''
            
            airline = flight_data.get('airline', {})
            departure = flight_data.get('departure', {})
            arrival = flight_data.get('arrival', {})
            
            params = (
                f"{flight_number}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                flight_number,
                flight_data.get('aircraft', {}).get('reg', ''),
                departure.get('airport', {}).get('iata', ''),
                arrival.get('airport', {}).get('iata', ''),
                departure.get('scheduledTime', {}).get('local', ''),
                departure.get('actualTime', {}).get('local', ''),
                arrival.get('scheduledTime', {}).get('local', ''),
                arrival.get('actualTime', {}).get('local', ''),
                flight_data.get('status', 'scheduled'),
                airline.get('icao', ''),
                airline.get('name', '')
            )
            
            self.db.execute_query(query, params)
        except Exception as e:
            print(f"Error storing flight: {e}")
    
    def _store_delays(self, airport_code: str, data: Dict):
        """Store delay statistics"""
        try:
            query = '''
            INSERT OR REPLACE INTO airport_delays 
            (airport_iata, delay_date, total_flights, delayed_flights, 
             avg_delay_min, median_delay_min, canceled_flights)
            VALUES (?, DATE('now'), ?, ?, ?, ?, ?)
            '''
            
            stats = data.get('statistics', {})
            flights = stats.get('flights', {})
            delays = stats.get('delays', {})
            
            params = (
                airport_code,
                flights.get('total', 0),
                flights.get('delayed', 0),
                delays.get('averageMinutes', 0),
                delays.get('medianMinutes', 0),
                flights.get('canceled', 0)
            )
            
            self.db.execute_query(query, params)
        except Exception as e:
            print(f"Error storing delays for {airport_code}: {e}")
    
    def get_stats(self) -> Dict:
        """Get fetcher statistics"""
        client_stats = self.client.get_stats()
        return {
            'last_fetch': self.last_fetch,
            'client_stats': client_stats
        }