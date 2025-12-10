"""
Data fetcher for 15 airports
"""

import time
from datetime import datetime
from typing import Dict, List, Optional
from curl_fetcher import CurlDataFetcher
from database import FlightDatabase
from config import AIRPORT_CODES, AIRPORT_GROUPS, AIRPORT_NAMES

class SmartDataFetcher:
    """Fetches data for 15 airports"""
    
    def __init__(self, db: FlightDatabase):
        self.fetcher = CurlDataFetcher()
        self.db = db
        self.last_fetch = None
        print(f"✅ Fetcher ready for {len(AIRPORT_CODES)} airports")
    
    def fetch_all_airport_data(self) -> Dict:
        """Fetch data for all 15 airports"""
        print("\n" + "="*60)
        print("🚀 FETCHING DATA FOR 15 AIRPORTS")
        print("="*60)
        
        start_time = time.time()
        
        try:
            # 1. Fetch airport basic info (all 15 airports)
            print(f"\n1️⃣ FETCHING AIRPORT INFORMATION...")
            print(f"   Processing {len(AIRPORT_CODES)} airports")
            
            airports_data = self.fetcher.get_multiple_airports_info(AIRPORT_CODES)
            
            airport_count = 0
            for code, data in airports_data.items():
                if data:
                    self._store_airport(code, data)
                    airport_count += 1
                    print(f"   ✅ {code}: {AIRPORT_NAMES.get(code, code)}")
            
            print(f"   📊 Stored {airport_count}/{len(AIRPORT_CODES)} airports")
            
            # 2. Fetch flight schedules (10 airports for speed)
            print(f"\n2️⃣ FETCHING FLIGHT SCHEDULES...")
            flight_airports = AIRPORT_CODES[:10]  # First 10 airports
            print(f"   Getting departures for {len(flight_airports)} airports")
            
            flights_data = self.fetcher.get_multiple_flight_schedules(flight_airports)
            
            total_flights = 0
            for code, data in flights_data.items():
                if data and 'data' in data:
                    flights = data['data'][:15]  # First 15 flights per airport
                    for flight in flights:
                        if self._store_flight(flight):
                            total_flights += 1
                    print(f"   ✈️ {code}: {len(data['data'])} flights found")
            
            print(f"   📊 Stored {total_flights} flights")
            
            # 3. Fetch delay stats (5 major airports only - tier 3)
            print(f"\n3️⃣ FETCHING DELAY STATISTICS...")
            delay_airports = AIRPORT_CODES[:5]  # First 5 airports
            print(f"   Getting delays for {len(delay_airports)} major airports")
            
            delays_data = self.fetcher.get_multiple_delay_stats(delay_airports)
            
            delay_count = 0
            for code, data in delays_data.items():
                if data:
                    self._store_delays(code, data)
                    delay_count += 1
                    print(f"   ⏱️ {code}: Delay data stored")
            
            elapsed = time.time() - start_time
            self.last_fetch = datetime.now()
            
            print("\n" + "="*60)
            print(f"✅ FETCH COMPLETE: {elapsed:.1f} seconds")
            print(f"   Airports: {airport_count}")
            print(f"   Flights: {total_flights}")
            print(f"   Delay Stats: {delay_count}")
            print("="*60)
            
            return {
                'success': True,
                'time': elapsed,
                'airports_fetched': airport_count,
                'flights_fetched': total_flights,
                'delays_fetched': delay_count,
                'stats': self.fetcher.get_stats()
            }
            
        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            return {
                'success': False,
                'error': str(e),
                'time': time.time() - start_time
            }
    
    def fetch_region_data(self, region: str) -> Dict:
        """Fetch data for a specific region"""
        if region not in AIRPORT_GROUPS:
            return {'success': False, 'error': f'Unknown region: {region}'}
        
        airports = AIRPORT_GROUPS[region]
        print(f"\n🌍 FETCHING {region.upper()} DATA ({len(airports)} airports)")
        
        start_time = time.time()
        
        try:
            # Fetch airport info
            airports_data = self.fetcher.get_multiple_airports_info(airports)
            
            airport_count = 0
            for code, data in airports_data.items():
                if data:
                    self._store_airport(code, data)
                    airport_count += 1
            
            # Fetch flight schedules
            flights_data = self.fetcher.get_multiple_flight_schedules(airports[:3])  # First 3
            
            flight_count = 0
            for code, data in flights_data.items():
                if data and 'data' in data:
                    for flight in data['data'][:10]:
                        if self._store_flight(flight):
                            flight_count += 1
            
            elapsed = time.time() - start_time
            
            return {
                'success': True,
                'region': region,
                'airports': airport_count,
                'flights': flight_count,
                'time': elapsed
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def search_flights(self, query: str) -> List[Dict]:
        """Search flights across all airports"""
        results = []
        
        try:
            # If it's a flight number
            if len(query) in [6, 7] and query[:2].isalpha():
                data = self.fetcher.get_flight_status(query)
                if data:
                    results.append(data)
            
            # If it's a route (e.g., DEL-BOM)
            elif '-' in query and len(query.split('-')) == 2:
                origin, dest = query.split('-')
                data = self.fetcher._run_curl(f"/flights/airports/iata/{origin}/departures")
                if data and 'data' in data:
                    for flight in data['data']:
                        arr_code = flight.get('arrival', {}).get('airport', {}).get('iata', '')
                        if arr_code == dest.upper():
                            results.append(flight)
            
            # If it's an airport code
            elif len(query) == 3 and query.isalpha():
                data = self.fetcher._run_curl(f"/flights/airports/iata/{query}/departures")
                if data and 'data' in data:
                    results.extend(data['data'][:20])
        
        except Exception as e:
            print(f"❌ Search error: {e}")
        
        return results
    
    def fetch_airport_details(self, airport_code: str) -> Dict:
        """Fetch details for specific airport"""
        details = {}
        
        try:
            # Basic info
            details['basic_info'] = self.fetcher._run_curl(f"/airports/iata/{airport_code}")
            
            # Current flights
            details['current_flights'] = self.fetcher._run_curl(
                f"/flights/airports/iata/{airport_code}/departures"
            )
            
            # Delays
            details['delays'] = self.fetcher._run_curl(f"/airports/iata/{airport_code}/delays")
            
        except Exception as e:
            print(f"❌ Airport details error: {e}")
        
        return details
    
    # ==================== STORAGE METHODS ====================
    
    def _store_airport(self, code: str, data: Dict) -> bool:
        """Store airport in database"""
        try:
            query = '''
            INSERT OR REPLACE INTO airport 
            (icao_code, iata_code, name, city, country, continent, 
             latitude, longitude, timezone, region)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            # Determine region
            region = 'Other'
            for reg, codes in AIRPORT_GROUPS.items():
                if code in codes:
                    region = reg
                    break
            
            params = (
                str(data.get('icao', code)),
                str(data.get('iata', code)),
                str(data.get('name', AIRPORT_NAMES.get(code, code))),
                str(data.get('municipalityName', '')),
                str(data.get('country', {}).get('name', '')),
                str(data.get('continent', '')),
                float(data.get('location', {}).get('lat', 0)),
                float(data.get('location', {}).get('lon', 0)),
                str(data.get('timeZone', '')),
                region
            )
            
            self.db.execute_query(query, params)
            return True
            
        except Exception as e:
            print(f"❌ Airport store error {code}: {e}")
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
            return True
            
        except Exception as e:
            print(f"❌ Flight store error: {e}")
            return False
    
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
            
        except Exception as e:
            print(f"❌ Delay store error {airport_code}: {e}")
    
    def get_stats(self) -> Dict:
        """Get fetcher statistics"""
        return {
            'last_fetch': self.last_fetch,
            'fetcher_stats': self.fetcher.get_stats()
        }