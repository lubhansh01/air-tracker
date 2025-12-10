"""
Intelligent data fetcher for Flight Analytics - DEBUG VERSION
"""

import time
from datetime import datetime
from typing import Dict, List, Optional
import traceback
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
        print(f"✅ Fetcher initialized with {len(AIRPORT_CODES)} airports")
    
    def fetch_dashboard_data(self) -> Dict:
        """Fetch all data needed for dashboard overview"""
        print("\n" + "="*50)
        print("🚀 STARTING DATA FETCH")
        print("="*50)
        
        start_time = time.time()
        
        # Limit to 3 airports for stability
        dashboard_airports = AIRPORT_CODES[:3]
        print(f"📌 Processing airports: {dashboard_airports}")
        
        results = {
            'airports': {},
            'flights': {},
            'delays': {}
        }
        
        try:
            # 1. Fetch airport basic info (parallel)
            print("\n1️⃣ FETCHING AIRPORT INFO...")
            airports_data = self.client.get_multiple_airports(dashboard_airports)
            results['airports'] = airports_data
            
            print(f"   Received data for {len([d for d in airports_data.values() if d])} airports")
            
            # Store airport data
            airport_count = 0
            for code, data in airports_data.items():
                if data:
                    self._store_airport(code, data)
                    airport_count += 1
            print(f"   ✅ Stored {airport_count} airports")
            
            # 2. Fetch flights for airports
            print("\n2️⃣ FETCHING FLIGHT SCHEDULES...")
            flight_airports = dashboard_airports[:2]
            print(f"   Getting flights for: {flight_airports}")
            
            flights_data = self.client.get_multiple_flights(flight_airports)
            results['flights'] = flights_data
            
            # Count flights
            total_flights = 0
            for airport_code, flights in flights_data.items():
                if flights and 'data' in flights:
                    flight_count = len(flights['data'])
                    print(f"   {airport_code}: {flight_count} flights")
                    total_flights += flight_count
                    
                    # Store flight data
                    stored = 0
                    for flight in flights['data'][:15]:  # Limit to 15 flights
                        if self._store_flight(flight):
                            stored += 1
                    print(f"   Stored {stored} flights from {airport_code}")
            
            print(f"   📊 Total flights found: {total_flights}")
            
            # 3. Fetch delays (only for first airport)
            print("\n3️⃣ FETCHING DELAY STATISTICS...")
            if dashboard_airports:
                main_airport = dashboard_airports[0]
                print(f"   Getting delays for: {main_airport}")
                
                delays = self.client.get_airport_delays(main_airport)
                results['delays'] = {main_airport: delays}
                
                if delays:
                    print(f"   ✅ Received delay data for {main_airport}")
                    self._store_delays(main_airport, delays)
                else:
                    print(f"   ⚠️ No delay data for {main_airport}")
            
            elapsed = time.time() - start_time
            self.last_fetch = datetime.now()
            
            print("\n" + "="*50)
            print(f"✅ DATA FETCH COMPLETE: {elapsed:.1f} seconds")
            print(f"   Airports: {airport_count}")
            print(f"   Flights: {total_flights}")
            print("="*50)
            
            return {
                'success': True,
                'time': elapsed,
                'airports_fetched': airport_count,
                'flights_fetched': total_flights,
                'api_stats': self.client.get_stats()
            }
            
        except Exception as e:
            print(f"\n❌ ERROR DURING DATA FETCH: {e}")
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e),
                'time': time.time() - start_time
            }
    
    def fetch_airport_details(self, airport_code: str) -> Dict:
        """Fetch detailed information for a specific airport"""
        print(f"\n🔍 Fetching details for {airport_code}...")
        
        details = {
            'basic_info': None,
            'current_flights': None,
            'delays': None
        }
        
        try:
            # Fetch basic info
            print(f"   Getting airport info...")
            details['basic_info'] = self.client.get_airport_info(airport_code)
            print(f"   ✅ Airport info: {'Received' if details['basic_info'] else 'Failed'}")
            
            # Fetch current flights
            print(f"   Getting flight schedules...")
            details['current_flights'] = self.client.get_airport_flights(
                airport_code, 'departures'
            )
            if details['current_flights'] and 'data' in details['current_flights']:
                print(f"   ✅ Flights: {len(details['current_flights']['data'])} found")
            else:
                print(f"   ⚠️ No flight data")
            
            # Fetch delays
            print(f"   Getting delay stats...")
            details['delays'] = self.client.get_airport_delays(airport_code)
            print(f"   ✅ Delay data: {'Received' if details['delays'] else 'None'}")
            
        except Exception as e:
            print(f"❌ Error fetching airport details: {e}")
        
        return details
    
    def search_flights(self, query: str) -> List[Dict]:
        """Search flights by various criteria"""
        print(f"\n🔎 Searching flights: '{query}'")
        results = []
        
        try:
            # Auto-detect search type
            if '-' in query and len(query.split('-')) == 2:
                print(f"   Detected route search")
                origin, destination = query.split('-')
                flights = self.client.get_airport_flights(origin, 'departures')
                if flights and 'data' in flights:
                    for flight in flights['data']:
                        arr_airport = flight.get('arrival', {}).get('airport', {}).get('iata', '')
                        if arr_airport == destination.upper():
                            results.append(flight)
                    print(f"   Found {len(results)} flights on route {origin}-{destination}")
            
            elif len(query) in [6, 7] and query[:2].isalpha():
                print(f"   Detected flight number search")
                flight_data = self.client.get_flight_status(query)
                if flight_data:
                    results.append(flight_data)
                    print(f"   Found flight {query}")
            
            elif len(query) in [3, 4] and query.isalpha():
                print(f"   Detected airport code search")
                flights = self.client.get_airport_flights(query)
                if flights and 'data' in flights:
                    results.extend(flights['data'][:20])
                    print(f"   Found {len(flights['data'])} flights at {query}")
            
            else:
                print(f"   Detected text search")
                for airport in AIRPORT_CODES[:2]:
                    flights = self.client.get_airport_flights(airport, 'departures')
                    if flights and 'data' in flights:
                        for flight in flights['data'][:10]:
                            airline_name = flight.get('airline', {}).get('name', '').lower()
                            if query.lower() in airline_name:
                                results.append(flight)
                print(f"   Found {len(results)} flights matching '{query}'")
        
        except Exception as e:
            print(f"❌ Error searching flights: {e}")
        
        print(f"   Total results: {len(results)}")
        return results
    
    def get_aircraft_info(self, registration: str) -> Optional[Dict]:
        """Get detailed aircraft information"""
        print(f"\n🛩️ Getting aircraft info: {registration}")
        try:
            result = self.client.get_aircraft_info(registration)
            print(f"   ✅ {'Found' if result else 'Not found'}")
            return result
        except Exception as e:
            print(f"❌ Error: {e}")
            return None
    
    def _store_airport(self, code: str, data: Dict) -> bool:
        """Store airport in database"""
        try:
            if not data:
                print(f"   ⚠️ No data for airport {code}")
                return False
            
            query = '''
            INSERT OR REPLACE INTO airport 
            (icao_code, iata_code, name, city, country, continent, 
             latitude, longitude, timezone)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            # Extract data with proper type conversion
            icao_code = str(data.get('icao', code)) if data.get('icao') else code
            iata_code = str(data.get('iata', code)) if data.get('iata') else code
            name = str(data.get('name', f'Airport {code}'))
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
            
            result = self.db.execute_query(query, params)
            print(f"   ✅ Stored airport: {code} ({name})")
            return True
            
        except Exception as e:
            print(f"❌ Error storing airport {code}: {e}")
            return False
    
    def _store_flight(self, flight_data: Dict) -> bool:
        """Store flight in database"""
        try:
            flight_number = flight_data.get('number')
            if not flight_number:
                return False
            
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
            return True
            
        except Exception as e:
            print(f"❌ Error storing flight {flight_data.get('number', 'unknown')}: {e}")
            return False
    
    def _store_delays(self, airport_code: str, data: Dict):
        """Store delay statistics"""
        try:
            if not data:
                print(f"   ⚠️ No delay data for {airport_code}")
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
            
            # Handle average delay
            avg_delay_raw = delays.get('averageMinutes')
            avg_delay_min = float(avg_delay_raw) if avg_delay_raw is not None else 0.0
            
            # Handle median delay
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
            print(f"   ✅ Stored delays for {airport_code}: "
                  f"{delayed_flights}/{total_flights} delayed")
            
        except Exception as e:
            print(f"❌ Error storing delays for {airport_code}: {e}")
    
    def get_stats(self) -> Dict:
        """Get fetcher statistics"""
        client_stats = self.client.get_stats()
        return {
            'last_fetch': self.last_fetch,
            'client_stats': client_stats
        }