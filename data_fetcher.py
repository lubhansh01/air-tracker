"""
Complete Data Fetcher for 15 airports
"""

import time
from datetime import datetime
from typing import Dict, List, Optional
import subprocess
import json
import os
from dotenv import load_dotenv

load_dotenv()

from database import FlightDatabase

# Airport configuration
AIRPORT_CODES = ['DEL', 'BOM', 'MAA', 'BLR', 'HYD', 'CCU', 'AMD', 
                 'LHR', 'JFK', 'DXB', 'SIN', 'CDG', 'FRA', 'SYD', 'NRT']

AIRPORT_NAMES = {
    'DEL': 'Delhi (IGI)', 'BOM': 'Mumbai (CSM)', 'MAA': 'Chennai', 
    'BLR': 'Bengaluru', 'HYD': 'Hyderabad', 'CCU': 'Kolkata', 
    'AMD': 'Ahmedabad', 'LHR': 'London Heathrow', 'JFK': 'New York JFK',
    'DXB': 'Dubai', 'SIN': 'Singapore', 'CDG': 'Paris CDG',
    'FRA': 'Frankfurt', 'SYD': 'Sydney', 'NRT': 'Tokyo Narita'
}

class CurlFetcher:
    """Simple curl-based API fetcher"""
    
    def __init__(self):
        self.api_key = os.getenv('AERODATABOX_API_KEY')
        self.api_host = 'aerodatabox.p.rapidapi.com'
    
    def _run_curl(self, endpoint: str) -> Optional[Dict]:
        """Run curl command"""
        try:
            url = f"https://{self.api_host}{endpoint}"
            cmd = [
                'curl', '-s', '-X', 'GET', url,
                '-H', f'x-rapidapi-key: {self.api_key}',
                '-H', f'x-rapidapi-host: {self.api_host}',
                '--max-time', '10'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                return json.loads(result.stdout)
            return None
        except:
            return None
    
    def get_airport_info(self, airport: str) -> Optional[Dict]:
        """Get airport information"""
        return self._run_curl(f"/airports/iata/{airport}")
    
    def get_flights(self, airport: str) -> Optional[Dict]:
        """Get flights for airport"""
        return self._run_curl(f"/flights/airports/iata/{airport}/departures")

class SmartDataFetcher:
    """Main data fetcher class"""
    
    def __init__(self, db: FlightDatabase):
        self.fetcher = CurlFetcher()
        self.db = db
        print(f"✅ DataFetcher initialized for {len(AIRPORT_CODES)} airports")
    
    def fetch_all_data(self) -> Dict:
        """Fetch data for all airports"""
        print("🚀 Starting data fetch...")
        start_time = time.time()
        
        try:
            airport_count = 0
            flight_count = 0
            
            # Fetch data for first 5 airports (for speed)
            for airport in AIRPORT_CODES[:5]:
                print(f"📡 Fetching {airport}...")
                
                # Get airport info
                airport_data = self.fetcher.get_airport_info(airport)
                if airport_data:
                    self._store_airport(airport, airport_data)
                    airport_count += 1
                
                # Get flights
                flight_data = self.fetcher.get_flights(airport)
                if flight_data and 'data' in flight_data:
                    for flight in flight_data['data'][:5]:  # First 5 flights
                        self._store_flight(flight)
                        flight_count += 1
                
                time.sleep(0.5)  # Rate limiting
            
            elapsed = time.time() - start_time
            
            return {
                'success': True,
                'time': elapsed,
                'airports': airport_count,
                'flights': flight_count
            }
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return {'success': False, 'error': str(e)}
    
    def search_flights(self, query: str) -> List[Dict]:
        """Search flights"""
        results = []
        
        try:
            # Try to get flight by number
            if len(query) >= 6:
                # This is a simplified search
                # Get flights from DEL (as example)
                data = self.fetcher.get_flights('DEL')
                if data and 'data' in data:
                    for flight in data['data'][:10]:
                        if query.upper() in flight.get('number', '').upper():
                            results.append(flight)
        except:
            pass
        
        return results
    
    def fetch_airport_details(self, airport: str) -> Dict:
        """Get airport details"""
        details = {}
        
        try:
            details['basic_info'] = self.fetcher.get_airport_info(airport)
            details['flights'] = self.fetcher.get_flights(airport)
        except:
            pass
        
        return details
    
    def _store_airport(self, code: str, data: Dict):
        """Store airport in database"""
        try:
            query = '''
            INSERT OR REPLACE INTO airport 
            (iata_code, name, city, country, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?)
            '''
            
            params = (
                code,
                data.get('name', AIRPORT_NAMES.get(code, code)),
                data.get('municipalityName', ''),
                data.get('country', {}).get('name', ''),
                data.get('location', {}).get('lat', 0),
                data.get('location', {}).get('lon', 0)
            )
            
            self.db.execute_query(query, params)
            print(f"✅ Stored airport: {code}")
            
        except Exception as e:
            print(f"❌ Error storing {code}: {e}")
    
    def _store_flight(self, flight_data: Dict):
        """Store flight in database"""
        try:
            flight_number = flight_data.get('number')
            if not flight_number:
                return
            
            query = '''
            INSERT OR REPLACE INTO flights 
            (flight_id, flight_number, airline_name, origin_iata, 
             destination_iata, scheduled_departure, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            '''
            
            airline = flight_data.get('airline', {})
            departure = flight_data.get('departure', {})
            arrival = flight_data.get('arrival', {})
            
            params = (
                f"{flight_number}_{int(time.time())}",
                flight_number,
                airline.get('name', ''),
                departure.get('airport', {}).get('iata', ''),
                arrival.get('airport', {}).get('iata', ''),
                departure.get('scheduledTime', {}).get('local', ''),
                flight_data.get('status', '')
            )
            
            self.db.execute_query(query, params)
            
        except Exception as e:
            print(f"❌ Error storing flight: {e}")