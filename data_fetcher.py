"""
Intelligent data fetcher for Flight Analytics - Fixed version
"""

import time
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from aerodatabox_client import AeroDataBoxClient
from database import FlightDatabase
from config import AIRPORT_CODES

class SmartDataFetcher:
    """Fetches data intelligently and stores in database"""
    
    def __init__(self, db: FlightDatabase):
        self.client = AeroDataBoxClient()
        self.db = db
        self.last_fetch = None
    
    def fetch_dashboard_data(self) -> Dict:
        """Fetch all data needed for dashboard overview"""
        print("🚀 Starting data fetch...")
        start_time = time.time()
        
        # Limit to 3 airports for stability
        dashboard_airports = AIRPORT_CODES[:3]
        
        results = {
            'airports': {},
            'flights': {},
            'delays': {}
        }
        
        try:
            # 1. Fetch airport basic info (parallel)
            print("🌎 Fetching airport info...")
            airports_data = self.client.get_multiple_airports(dashboard_airports)
            results['airports'] = airports_data
            
            # Store airport data
            for code, data in airports_data.items():
                if data:
                    self._store_airport(code, data)
            
            # 2. Fetch flights for airports
            print("✈️ Fetching flight schedules...")
            flight_airports = dashboard_airports[:2]
            flights_data = self.client.get_multiple_flights(flight_airports)
            results['flights'] = flights_data
            
            # Store flight data
            for airport_code, flights in flights_data.items():
                if flights and 'data' in flights:
                    for flight in flights['data'][:15]:  # Limit to 15 flights
                        self._store_flight(flight)
            
            # 3. Fetch delays (only for first airport)
            print("⏱️ Fetching delay statistics...")
            if dashboard_airports:
                delays = self.client.get_airport_delays(dashboard_airports[0])
                results['delays'] = {dashboard_airports[0]: delays}
                
                if delays:
                    self._store_delays(dashboard_airports[0], delays)
            
            elapsed = time.time() - start_time
            self.last_fetch = datetime.now()
            
            flights_count = sum(len(f.get('data', [])) if f else 0 
                              for f in flights_data.values())
            
            print(f"✅ Data fetch completed in {elapsed:.1f} seconds")
            
            return {
                'success': True,
                'time': elapsed,
                'airports_fetched': len([d for d in airports_data.values() if d]),
                'flights_fetched': flights_count,
                'api_stats': self.client.get_stats()
            }
            
        except Exception as e:
            print(f"❌ Error during data fetch: {e}")
            return {
                'success': False,
                'error': str(e),
                'time': time.time() - start_time
            }
    
    def fetch_airport_details(self, airport_code: str) -> Dict:
        """Fetch detailed information for a specific airport"""
        details = {
            'basic_info': None,
            'current_flights': None,
            'delays': None
        }
        
        try:
            # Fetch basic info
            details['basic_info'] = self.client.get_airport_info(airport_code)
            
            # Fetch current flights
            details['current_flights'] = self.client.get_airport_flights(
                airport_code, 'departures'
            )
            
            # Fetch delays (only if airport is in our main list)
            if airport_code in AIRPORT_CODES[:4]:
                details['delays'] = self.client.get_airport_delays(airport_code)
            
        except Exception as e:
            print(f"❌ Error fetching airport details: {e}")
        
        return details
    
    def search_flights(self, query: str) -> List[Dict]:
        """Search flights by various criteria"""
        results = []
        
        try:
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
                    results.extend(flights['data'][:20])  # Limit to 20
            
            else:
                # Text search in airline names
                for airport in AIRPORT_CODES[:2]:
                    flights = self.client.get_airport_flights(airport, 'departures')
                    if flights and 'data' in flights:
                        for flight in flights['data'][:10]:
                            airline_name = flight.get('airline', {}).get('name', '').lower()
                            if query.lower() in airline_name:
                                results.append(flight)
        
        except Exception as e:
            print(f"❌ Error searching flights: {e}")
        
        return results
    
    def get_aircraft_info(self, registration: str) -> Optional[Dict]:
        """Get detailed aircraft information"""
        try:
            return self.client.get_aircraft_info(registration)
        except Exception as e:
            print(f"❌ Error fetching aircraft info: {e}")
            return None
    
    def _store_airport(self, code: str, data: Dict):
        """Store airport in database - FIXED VERSION"""
        try:
            query = '''
            INSERT OR REPLACE INTO airport 
            (icao_code, iata_code, name, city, country, continent, 
             latitude, longitude, timezone)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            # Extract data with proper type conversion
            icao_code = str(data.get('icao', code)) if data.get('icao') else code
            iata_code = str(data.get('iata', code)) if data.get('iata') else code
            name = str(data.get('name', ''))
            city = str(data.get('municipalityName', ''))
            country = str(data.get('country', {}).get('name', ''))
            continent = str(data.get('continent', ''))
            
            # Handle location data
            location = data.get('location', {})
            latitude = float(location.get('lat', 0))
            longitude = float(location.get('lon', 0))
            
            timezone = str(data.get('timeZone', ''))
            
            params = (
                icao_code,
                iata_code,
                name,
                city,
                country,
                continent,
                latitude,
                longitude,
                timezone
            )
            
            self.db.execute_query(query, params)
            print(f"✅ Stored airport: {code}")
            
        except Exception as e:
            print(f"❌ Error storing airport {code}: {e}")
    
    def _store_flight(self, flight_data: Dict):
        """Store flight in database - FIXED VERSION"""
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
            
            # Extract data with proper type conversion
            flight_id = f"{flight_number}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            aircraft_reg = str(flight_data.get('aircraft', {}).get('reg', ''))
            origin_iata = str(departure.get('airport', {}).get('iata', ''))
            dest_iata = str(arrival.get('airport', {}).get('iata', ''))
            scheduled_dep = str(departure.get('scheduledTime', {}).get('local', ''))
            actual_dep = str(departure.get('actualTime', {}).get('local', ''))
            scheduled_arr = str(arrival.get('scheduledTime', {}).get('local', ''))
            actual_arr = str(arrival.get('actualTime', {}).get('local', ''))
            status = str(flight_data.get('status', 'scheduled'))
            airline_code = str(airline.get('icao', ''))
            airline_name = str(airline.get('name', ''))
            
            params = (
                flight_id,
                flight_number,
                aircraft_reg,
                origin_iata,
                dest_iata,
                scheduled_dep,
                actual_dep,
                scheduled_arr,
                actual_arr,
                status,
                airline_code,
                airline_name
            )
            
            self.db.execute_query(query, params)
            
        except Exception as e:
            print(f"❌ Error storing flight: {e}")
    
    def _store_delays(self, airport_code: str, data: Dict):
        """Store delay statistics - FIXED VERSION"""
        try:
            if not data:
                return
            
            query = '''
            INSERT OR REPLACE INTO airport_delays 
            (airport_iata, delay_date, total_flights, delayed_flights, 
             avg_delay_min, median_delay_min, canceled_flights)
            VALUES (?, DATE('now'), ?, ?, ?, ?, ?)
            '''
            
            # Extract data with proper type conversion
            stats = data.get('statistics', {})
            flights = stats.get('flights', {})
            delays = stats.get('delays', {})
            
            # Convert all values to proper types
            total_flights = int(flights.get('total', 0))
            delayed_flights = int(flights.get('delayed', 0))
            
            # Handle average delay - ensure it's a float
            avg_delay_raw = delays.get('averageMinutes')
            avg_delay_min = float(avg_delay_raw) if avg_delay_raw is not None else 0.0
            
            # Handle median delay - ensure it's a float
            median_delay_raw = delays.get('medianMinutes')
            median_delay_min = float(median_delay_raw) if median_delay_raw is not None else 0.0
            
            canceled_flights = int(flights.get('canceled', 0))
            
            params = (
                str(airport_code),
                total_flights,
                delayed_flights,
                avg_delay_min,
                median_delay_min,
                canceled_flights
            )
            
            self.db.execute_query(query, params)
            print(f"✅ Stored delays for: {airport_code}")
            
        except Exception as e:
            print(f"❌ Error storing delays for {airport_code}: {e}")
            print(f"   Data received: {data}")
    
    def get_stats(self) -> Dict:
        """Get fetcher statistics"""
        client_stats = self.client.get_stats()
        return {
            'last_fetch': self.last_fetch,
            'client_stats': client_stats
        }