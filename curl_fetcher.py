"""
CURL-based fetcher for 15 airports
"""

import subprocess
import json
import os
import time
from typing import Dict, List, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import AIRPORT_CODES, PARALLEL_LIMITS, CACHE_TTL

class CurlDataFetcher:
    """Fetches data for multiple airports using curl"""
    
    def __init__(self):
        self.api_key = os.getenv('AERODATABOX_API_KEY')
        self.api_host = os.getenv('AERODATABOX_API_HOST', 'aerodatabox.p.rapidapi.com')
        self.cache = {}
        self.stats = {
            'total_requests': 0,
            'successful': 0,
            'failed': 0,
            'cache_hits': 0
        }
        
        print(f"✅ cURL Fetcher initialized for {len(AIRPORT_CODES)} airports")
    
    def _run_curl(self, endpoint: str) -> Optional[Dict]:
        """Execute a curl command"""
        cache_key = endpoint
        
        # Check cache
        if cache_key in self.cache:
            cached_time, data = self.cache[cache_key]
            if time.time() - cached_time < CACHE_TTL:
                self.stats['cache_hits'] += 1
                return data
        
        try:
            curl_cmd = [
                'curl',
                '--request', 'GET',
                '--url', f'https://{self.api_host}{endpoint}',
                '--header', f'x-rapidapi-key: {self.api_key}',
                '--header', f'x-rapidapi-host: {self.api_host}',
                '--silent',
                '--max-time', '15'
            ]
            
            self.stats['total_requests'] += 1
            
            result = subprocess.run(
                curl_cmd,
                capture_output=True,
                text=True,
                timeout=20
            )
            
            if result.returncode == 0:
                try:
                    data = json.loads(result.stdout)
                    self.cache[cache_key] = (time.time(), data)
                    self.stats['successful'] += 1
                    return data
                except json.JSONDecodeError:
                    print(f"❌ JSON error for {endpoint}")
                    self.stats['failed'] += 1
                    return None
            else:
                print(f"❌ Curl error for {endpoint}: {result.stderr[:100]}")
                self.stats['failed'] += 1
                return None
                
        except Exception as e:
            print(f"❌ Error {endpoint}: {e}")
            self.stats['failed'] += 1
            return None
    
    # ==================== BATCH METHODS FOR 15 AIRPORTS ====================
    
    def get_multiple_airports_info(self, airport_codes: List[str]) -> Dict[str, Optional[Dict]]:
        """Get info for multiple airports in parallel"""
        results = {}
        
        def fetch_one(code):
            endpoint = f"/airports/iata/{code}"
            return code, self._run_curl(endpoint)
        
        with ThreadPoolExecutor(max_workers=PARALLEL_LIMITS['airport_info']) as executor:
            futures = {executor.submit(fetch_one, code): code for code in airport_codes}
            for future in as_completed(futures):
                code, data = future.result()
                results[code] = data
                time.sleep(0.3)  # Small delay between requests
        
        return results
    
    def get_multiple_flight_schedules(self, airport_codes: List[str]) -> Dict[str, Optional[Dict]]:
        """Get flight schedules for multiple airports"""
        results = {}
        
        def fetch_one(code):
            endpoint = f"/flights/airports/iata/{code}/departures"
            return code, self._run_curl(endpoint)
        
        with ThreadPoolExecutor(max_workers=PARALLEL_LIMITS['flight_schedules']) as executor:
            futures = {executor.submit(fetch_one, code): code for code in airport_codes}
            for future in as_completed(futures):
                code, data = future.result()
                results[code] = data
                time.sleep(0.5)  # Longer delay for flight data
        
        return results
    
    def get_multiple_delay_stats(self, airport_codes: List[str]) -> Dict[str, Optional[Dict]]:
        """Get delay stats for multiple airports"""
        results = {}
        
        def fetch_one(code):
            endpoint = f"/airports/iata/{code}/delays"
            return code, self._run_curl(endpoint)
        
        with ThreadPoolExecutor(max_workers=PARALLEL_LIMITS['delay_stats']) as executor:
            futures = {executor.submit(fetch_one, code): code for code in airport_codes}
            for future in as_completed(futures):
                code, data = future.result()
                results[code] = data
                time.sleep(1)  # Longest delay for tier 3 endpoints
        
        return results
    
    def get_flight_status(self, flight_number: str) -> Optional[Dict]:
        """Get flight status"""
        date = datetime.now().strftime('%Y-%m-%d')
        endpoint = f"/flights/number/{flight_number}/{date}"
        return self._run_curl(endpoint)
    
    def get_aircraft_info(self, registration: str) -> Optional[Dict]:
        """Get aircraft info"""
        endpoint = f"/aircrafts/reg/{registration}"
        return self._run_curl(endpoint)
    
    def clear_cache(self):
        """Clear cache"""
        self.cache.clear()
        print("✅ Cache cleared")
    
    def get_stats(self) -> Dict:
        """Get fetcher statistics"""
        return self.stats.copy()