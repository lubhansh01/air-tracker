"""
CURL-based data fetcher for AeroDataBox API
Uses direct curl commands from the API platform
"""

import subprocess
import json
import os
import time
from typing import Dict, List, Optional, Any
from datetime import datetime

class CurlDataFetcher:
    """Fetches data using curl commands"""
    
    def __init__(self):
        self.api_key = os.getenv('AERODATABOX_API_KEY')
        self.api_host = os.getenv('AERODATABOX_API_HOST', 'aerodatabox.p.rapidapi.com')
        self.stats = {
            'requests': 0,
            'success': 0,
            'failures': 0
        }
    
    def _run_curl(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Execute a curl command and return JSON response"""
        try:
            # Build curl command
            url = f"https://{self.api_host}{endpoint}"
            
            curl_cmd = [
                'curl',
                '--request', 'GET',
                '--url', url,
                '--header', f'x-rapidapi-key: {self.api_key}',
                '--header', f'x-rapidapi-host: {self.api_host}',
                '--silent',
                '--max-time', '30'  # 30 second timeout
            ]
            
            # Add parameters if provided
            if params:
                for key, value in params.items():
                    if value:
                        curl_cmd.extend(['--data-urlencode', f'{key}={value}'])
            
            self.stats['requests'] += 1
            print(f"🌐 Curl: {endpoint.split('/')[-1]}")
            
            # Execute curl command
            result = subprocess.run(
                curl_cmd,
                capture_output=True,
                text=True,
                timeout=35
            )
            
            if result.returncode == 0:
                try:
                    data = json.loads(result.stdout)
                    self.stats['success'] += 1
                    return data
                except json.JSONDecodeError:
                    print(f"❌ JSON parse error: {result.stdout[:100]}")
                    self.stats['failures'] += 1
                    return None
            else:
                print(f"❌ Curl error: {result.stderr}")
                self.stats['failures'] += 1
                return None
                
        except subprocess.TimeoutExpired:
            print(f"❌ Timeout fetching {endpoint}")
            self.stats['failures'] += 1
            return None
        except Exception as e:
            print(f"❌ Error: {e}")
            self.stats['failures'] += 1
            return None
    
    # ==================== AIRPORT ENDPOINTS ====================
    
    def get_airport_info(self, airport_code: str) -> Optional[Dict]:
        """Get airport information (Tier 1)"""
        endpoint = f"/airports/iata/{airport_code}"
        return self._run_curl(endpoint)
    
    def get_airport_runways(self, airport_code: str) -> Optional[Dict]:
        """Get airport runways (Tier 1)"""
        endpoint = f"/airports/iata/{airport_code}/runways"
        return self._run_curl(endpoint)
    
    # ==================== FLIGHT ENDPOINTS ====================
    
    def get_airport_schedule(self, airport_code: str, direction: str = "departures") -> Optional[Dict]:
        """Get airport FIDS (Flight Information Display System) (Tier 2)"""
        endpoint = f"/flights/airports/iata/{airport_code}/{direction}"
        return self._run_curl(endpoint)
    
    def get_flight_status(self, flight_number: str, date: str = None) -> Optional[Dict]:
        """Get flight status (Tier 2)"""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        endpoint = f"/flights/number/{flight_number}/{date}"
        return self._run_curl(endpoint)
    
    def search_flights_by_term(self, search_term: str) -> Optional[Dict]:
        """Search flights by term (Tier 2)"""
        endpoint = "/flights/search/term"
        params = {'q': search_term}
        return self._run_curl(endpoint, params)
    
    # ==================== AIRCRAFT ENDPOINTS ====================
    
    def get_aircraft_info(self, registration: str) -> Optional[Dict]:
        """Get aircraft information (Tier 1)"""
        endpoint = f"/aircrafts/reg/{registration}"
        return self._run_curl(endpoint)
    
    # ==================== STATISTICAL ENDPOINTS ====================
    
    def get_airport_delays(self, airport_code: str) -> Optional[Dict]:
        """Get airport delays (Tier 3)"""
        endpoint = f"/airports/iata/{airport_code}/delays"
        return self._run_curl(endpoint)
    
    def get_global_delays(self) -> Optional[Dict]:
        """Get global delays (Tier 3)"""
        endpoint = "/airports/delays"
        return self._run_curl(endpoint)
    
    # ==================== MISC ENDPOINTS ====================
    
    def get_airport_time(self, airport_code: str) -> Optional[Dict]:
        """Get local time at airport (Tier 1)"""
        endpoint = f"/airports/iata/{airport_code}/time/local"
        return self._run_curl(endpoint)
    
    def get_countries(self) -> Optional[Dict]:
        """Get all countries (Tier 1)"""
        endpoint = "/countries"
        return self._run_curl(endpoint)
    
    def get_stats(self) -> Dict:
        """Get fetcher statistics"""
        return self.stats.copy()