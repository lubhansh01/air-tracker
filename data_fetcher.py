"""
Data fetcher using cURL commands
"""

import time
from datetime import datetime
from typing import Dict, List, Optional
from curl_fetcher import CurlDataFetcher
from database import FlightDatabase

class SmartDataFetcher:
    """Fetches data using cURL commands"""
    
    def __init__(self, db: FlightDatabase):
        self.fetcher = CurlDataFetcher()
        self.db = db
        self.last_fetch = None
        print("✅ cURL Fetcher initialized")
    
    def fetch_dashboard_data(self) -> Dict:
        """Fetch all data needed for dashboard"""
        print("\n" + "="*50)
        print("🚀 STARTING DATA FETCH (cURL)")
        print("="*50)
        
        start_time = time.time()
        airports_to_fetch = ['DEL', 'BOM', 'LHR']  # Major airports
        
        try:
            # 1. Fetch airport info
            print("\n1️⃣ FETCHING AIRPORT INFO...")
            airport_count = 0
            for airport in airports_to_fetch:
                data = self.fetcher.get_airport_info(airport)
                if data:
                    self._store_airport(airport, data)
                    airport_count += 1
                    time.sleep(0.5)  # Rate limiting
            print(f"   ✅ Stored {airport_count} airports")
            
            # 2. Fetch flight schedules
            print("\n2️⃣ FETCHING FLIGHT SCHEDULES...")
            flight_count = 0
            for airport in airports_to_fetch[:2]:  # First 2 only
                data = self.fetcher.get_airport_schedule(airport, 'departures')
                if data and 'data' in data:
                    for flight in data['data'][:10]:  # First 10 flights
                        self._store_flight(flight)
                        flight_count += 1
                    print(f"   {airport}: {len(data['data'])} flights found")
                time.sleep(1)  # Rate limiting
            print(f"   ✅ Stored {flight_count} flights")
            
            # 3. Fetch delays
            print("\n3️⃣ FETCHING DELAY STATISTICS...")
            if airports_to_fetch:
                data = self.fetcher.get_airport_delays(airports_to_fetch[0])
                if data:
                    self._store_delays(airports_to_fetch[0], data)
                    print(f"   ✅ Stored delays for {airports_to_fetch[0]}")
                time.sleep(1)
            
            elapsed = time.time() - start_time
            self.last_fetch = datetime.now()
            
            print("\n" + "="*50)
            print(f"✅ DATA FETCH COMPLETE: {elapsed:.1f} seconds")
            print("="*50)
            
            return {
                'success': True,
                'time': elapsed,
                'airports_fetched': airport_count,
                'flights_fetched': flight_count,
                'stats': self.fetcher.get_stats()
            }
            
        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            return {
                'success': False,
                'error': str(e),
                'time': time.time() - start_time
            }
    
    def search_flights(self, query: str) -> List[Dict]:
        """Search flights"""
        results = []
        
        try:
            # Try different search methods
            if '-' in query and len(query.split('-')) == 2:
                # Route search
                origin, dest = query.split('-')
                data = self.fetcher.get_airport_schedule(origin, 'departures')
                if data and 'data' in data:
                    for flight in data['data']:
                        arr_code = flight.get('arrival', {}).get('airport', {}).get('iata', '')
                        if arr_code == dest.upper():
                            results.append(flight)
            
            elif len(query) in [6, 7] and query[:2].isalpha():
                # Flight number
                data = self.fetcher.get_flight_status(query)
                if data:
                    results.append(data)
            
            elif len(query) == 3 and query.isalpha():
                # Airport code
                data = self.fetcher.get_airport_schedule(query, 'departures')
                if data and 'data' in data:
                    results.extend(data['data'][:20])
            
            else:
                # Text search
                data = self.fetcher.search_flights_by_term(query)
                if data and 'data' in data:
                    results.extend(data['data'][:15])
                    
        except Exception as e:
            print(f"❌ Search error: {e}")
        
        return results
    
    def fetch_airport_details(self, airport_code: str) -> Dict:
        """Fetch airport details"""
        details = {}
        
        try:
            # Basic info
            details['basic_info'] = self.fetcher.get_airport_info(airport_code)
            time.sleep(0.5)
            
            # Current flights
            details['current_flights'] = self.fetcher.get_airport_schedule(airport_code, 'departures')
            time.sleep(0.5)
            
            # Delays
            details['delays'] = self.fetcher.get_airport_delays(airport_code)
            
        except Exception as e:
            print(f"❌ Airport details error: {e}")
        
        return details
    
    # Storage methods remain the same as before
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
                str(data.get('icao', code)),
                str(data.get('iata', code)),
                str(data.get('name', '')),
                str(data.get('municipalityName', '')),
                str(data.get('country', {}).get('name', '')),
                str(data.get('continent', '')),
                float(data.get('location', {}).get('lat', 0)),
                float(data.get('location', {}).get('lon', 0)),
                str(data.get('timeZone', ''))
            )
            
            self.db.execute_query(query, params)
            print(f"   ✅ Airport: {code}")
            
        except Exception as e:
            print(f"❌ Airport store error: {e}")
    
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
                str(flight_number),
                str(flight_data.get('aircraft', {}).get('reg', '')),
                str(departure.get('airport', {}).get('iata', '')),
                str(arrival.get('airport', {}).get('iata', '')),
                str(departure.get('scheduledTime', {}).get('local', '')),
                str(departure.get('actualTime', {}).get('local', '')),
                str(arrival.get('scheduledTime', {}).get('local', '')),
                str(arrival.get('actualTime', {}).get('local', '')),
                str(flight_data.get('status', 'scheduled')),
                str(airline.get('icao', '')),
                str(airline.get('name', ''))
            )
            
            self.db.execute_query(query, params)
            
        except Exception as e:
            print(f"❌ Flight store error: {e}")
    
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
                str(airport_code),
                int(flights.get('total', 0)),
                int(flights.get('delayed', 0)),
                float(delays.get('averageMinutes', 0)),
                float(delays.get('medianMinutes', 0)),
                int(flights.get('canceled', 0))
            )
            
            self.db.execute_query(query, params)
            print(f"   ✅ Delays: {airport_code}")
            
        except Exception as e:
            print(f"❌ Delay store error: {e}")