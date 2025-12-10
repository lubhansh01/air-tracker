"""
AeroDataBox API Client
"""

import requests
import time
import json
import hashlib
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from config import (
    HEADERS, build_url, get_code_type, 
    FETCH_STRATEGY, AIRPORT_CODES
)

class AeroDataBoxClient:
    """Client for AeroDataBox API with intelligent caching"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.cache = {}
        self.request_log = []
        self.stats = {
            'total_requests': 0,
            'successful': 0,
            'failed': 0,
            'cache_hits': 0
        }
    
    def _rate_limit(self):
        """Implement rate limiting"""
        current_time = time.time()
        
        # Remove requests older than 60 seconds
        self.request_log = [ts for ts in self.request_log 
                          if current_time - ts < 60]
        
        # Limit to 30 requests per minute
        if len(self.request_log) >= 30:
            sleep_time = 60 - (current_time - self.request_log[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        self.request_log.append(current_time)
        time.sleep(FETCH_STRATEGY['rate_limit_delay'])
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make API request with caching"""
        cache_key = hashlib.md5(
            f"{endpoint}:{json.dumps(params) if params else ''}".encode()
        ).hexdigest()
        
        # Check cache
        if cache_key in self.cache:
            cached_time, data = self.cache[cache_key]
            if time.time() - cached_time < FETCH_STRATEGY['cache_ttl']:
                self.stats['cache_hits'] += 1
                return data
        
        self._rate_limit()
        
        for attempt in range(FETCH_STRATEGY['max_retries']):
            try:
                response = self.session.get(
                    endpoint,
                    params=params,
                    timeout=FETCH_STRATEGY['timeout']
                )
                response.raise_for_status()
                
                data = response.json()
                
                # Cache successful response
                self.cache[cache_key] = (time.time(), data)
                self.stats['total_requests'] += 1
                self.stats['successful'] += 1
                
                return data
                
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:  # Rate limit
                    sleep_time = 2 ** attempt
                    time.sleep(sleep_time)
                    continue
                elif response.status_code == 404:
                    return None
                else:
                    print(f"HTTP error: {e}")
                    
            except requests.exceptions.RequestException as e:
                print(f"Request error: {e}")
                if attempt < FETCH_STRATEGY['max_retries'] - 1:
                    time.sleep(1)
                    continue
        
        self.stats['total_requests'] += 1
        self.stats['failed'] += 1
        return None
    
    # ==================== AIRPORT METHODS ====================
    
    def get_airport_info(self, airport_code: str) -> Optional[Dict]:
        """Get airport information"""
        code_type = get_code_type(airport_code)
        url = build_url('AIRPORT_INFO', 
                       codeType=code_type, code=airport_code)
        return self._make_request(url)
    
    def get_airport_time(self, airport_code: str) -> Optional[Dict]:
        """Get local time at airport"""
        code_type = get_code_type(airport_code)
        url = build_url('AIRPORT_TIME', 
                       codeType=code_type, code=airport_code)
        return self._make_request(url)
    
    # ==================== AIRCRAFT METHODS ====================
    
    def get_aircraft_info(self, registration: str) -> Optional[Dict]:
        """Get aircraft information by registration"""
        url = build_url('AIRCRAFT_INFO', 
                       searchBy='reg', searchParam=registration)
        return self._make_request(url)
    
    # ==================== FLIGHT METHODS ====================
    
    def get_flight_status(self, flight_number: str, 
                         date: str = None) -> Optional[Dict]:
        """Get flight status"""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        url = build_url('FLIGHT_STATUS_DATE',
                       searchBy='number', 
                       searchParam=flight_number,
                       dateLocal=date)
        return self._make_request(url)
    
    def get_airport_flights(self, airport_code: str, 
                           direction: str = None) -> Optional[Dict]:
        """Get airport FIDS (Flight Information Display System)"""
        code_type = get_code_type(airport_code)
        url = build_url('AIRPORT_FIDS',
                       codeType=code_type, code=airport_code)
        
        params = {}
        if direction:
            params['direction'] = direction
        
        return self._make_request(url, params)
    
    # ==================== STATISTICAL METHODS ====================
    
    def get_airport_delays(self, airport_code: str) -> Optional[Dict]:
        """Get airport delay statistics"""
        code_type = get_code_type(airport_code)
        url = build_url('AIRPORT_DELAYS',
                       codeType=code_type, code=airport_code)
        return self._make_request(url)
    
    def get_global_delays(self) -> Optional[Dict]:
        """Get global delay statistics"""
        url = build_url('GLOBAL_DELAYS')
        return self._make_request(url)
    
    # ==================== MISC METHODS ====================
    
    def get_airport_weather(self, airport_code: str) -> Optional[Dict]:
        """Get airport weather"""
        code_type = get_code_type(airport_code)
        url = build_url('AIRPORT_WEATHER',
                       codeType=code_type, code=airport_code)
        return self._make_request(url)
    
    def get_distance_time(self, from_airport: str, 
                         to_airport: str) -> Optional[Dict]:
        """Get distance and flight time between airports"""
        from_type = get_code_type(from_airport)
        to_type = get_code_type(to_airport)
        
        url = build_url('DISTANCE_TIME',
                       codeType=from_type, codeFrom=from_airport,
                       codeTo=to_airport)
        return self._make_request(url)
    
    # ==================== BATCH METHODS ====================
    
    def get_multiple_airports(self, airport_codes: List[str]) -> Dict[str, Optional[Dict]]:
        """Get multiple airport info in parallel"""
        results = {}
        
        def fetch_one(code):
            return code, self.get_airport_info(code)
        
        with ThreadPoolExecutor(max_workers=FETCH_STRATEGY['parallel_workers']) as executor:
            futures = {executor.submit(fetch_one, code): code for code in airport_codes}
            for future in as_completed(futures):
                code, data = future.result()
                results[code] = data
        
        return results
    
    def get_multiple_flights(self, airport_codes: List[str]) -> Dict[str, Optional[Dict]]:
        """Get flights for multiple airports"""
        results = {}
        
        def fetch_one(code):
            return code, self.get_airport_flights(code, 'departures')
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(fetch_one, code): code for code in airport_codes}
            for future in as_completed(futures):
                code, data = future.result()
                results[code] = data
                time.sleep(0.5)
        
        return results
    
    def clear_cache(self):
        """Clear all cached data"""
        self.cache.clear()
        print("Cache cleared")
    
    def get_stats(self) -> Dict:
        """Get client statistics"""
        return self.stats.copy()