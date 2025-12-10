"""
Core API client for AeroDataBox with intelligent caching, rate limiting,
and parallel processing optimized for the Swagger endpoints.
"""
import requests
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import aiohttp
from config import HEADERS, get_endpoint_url, DATA_LIMITS, PARALLEL_WORKERS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AeroDataBoxClient:
    """Main client for interacting with AeroDataBox API"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.cache = {}
        self.request_timestamps = []
        self.rate_limit_window = 60  # 60-second window
        self.max_requests_per_minute = 30  # Conservative limit
        
    def _rate_limit(self):
        """Implement intelligent rate limiting"""
        now = time.time()
        
        # Remove old timestamps
        self.request_timestamps = [
            ts for ts in self.request_timestamps 
            if now - ts < self.rate_limit_window
        ]
        
        # Check if we're at the limit
        if len(self.request_timestamps) >= self.max_requests_per_minute:
            sleep_time = self.rate_limit_window - (now - self.request_timestamps[0])
            if sleep_time > 0:
                logger.info(f"Rate limit approaching, sleeping {sleep_time:.1f}s")
                time.sleep(sleep_time)
        
        self.request_timestamps.append(now)
        
        # Add small delay between requests
        time.sleep(0.1)
    
    def _get_cache_key(self, endpoint: str, params: Dict = None) -> str:
        """Generate cache key for endpoint and parameters"""
        key_data = f"{endpoint}:{json.dumps(params, sort_keys=True) if params else ''}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _make_request(self, endpoint_name: str, **kwargs) -> Optional[Dict]:
        """
        Make API request with caching and retry logic
        
        Args:
            endpoint_name: Name from ENDPOINTS config
            **kwargs: Path parameters for the endpoint
            
        Returns:
            JSON response as dict or None if failed
        """
        cache_key = self._get_cache_key(endpoint_name, kwargs)
        
        # Check cache
        if cache_key in self.cache:
            cache_entry = self.cache[cache_key]
            if time.time() - cache_entry['timestamp'] < DATA_LIMITS['cache_duration_seconds']:
                logger.debug(f"Cache hit for {endpoint_name}")
                return cache_entry['data']
        
        url = get_endpoint_url(endpoint_name, **kwargs)
        
        for attempt in range(DATA_LIMITS['max_retries']):
            try:
                self._rate_limit()
                
                logger.info(f"Requesting {endpoint_name} (attempt {attempt + 1})")
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                
                data = response.json()
                
                # Cache successful response
                self.cache[cache_key] = {
                    'data': data,
                    'timestamp': time.time()
                }
                
                return data
                
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:  # Rate limited
                    sleep_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Rate limited, sleeping {sleep_time}s")
                    time.sleep(sleep_time)
                    continue
                elif response.status_code == 404:
                    logger.warning(f"Endpoint not found: {endpoint_name}")
                    return None
                else:
                    logger.error(f"HTTP error for {endpoint_name}: {e}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed for {endpoint_name}: {e}")
                if attempt < DATA_LIMITS['max_retries'] - 1:
                    time.sleep(DATA_LIMITS['retry_delay'])
                    continue
            
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for {endpoint_name}: {e}")
        
        return None
    
    # ==================== SPECIFIC ENDPOINT METHODS ====================
    
    def get_airport_info(self, code: str) -> Optional[Dict]:
        """🌎 Get Airport information by IATA/ICAO code"""
        return self._make_request('AIRPORT_INFO', code=code)
    
    def get_airport_schedule(self, code: str, direction: str = "departures") -> Optional[Dict]:
        """🗓️ Get airport schedule (FIDS)"""
        return self._make_request('AIRPORT_SCHEDULE', code=code, direction=direction)
    
    def get_aircraft_info(self, registration: str) -> Optional[Dict]:
        """✈️ Get aircraft information by registration"""
        return self._make_request('AIRCRAFT_INFO', registration=registration)
    
    def get_airport_delays(self, code: str) -> Optional[Dict]:
        """📊 Get airport delay statistics"""
        return self._make_request('AIRPORT_DELAYS', code=code)
    
    def get_flight_status(self, flight_number: str, date: str = None) -> Optional[Dict]:
        """🗓️ Get flight status for specific date"""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        return self._make_request('FLIGHT_STATUS', flightNumber=flight_number, date=date)
    
    def get_airport_time(self, code: str) -> Optional[Dict]:
        """Get local time at airport"""
        return self._make_request('AIRPORT_TIME', code=code)
    
    def search_flights_by_term(self, term: str) -> Optional[Dict]:
        """Search flight numbers by term"""
        return self._make_request('SEARCH_FLIGHTS', term=term)
    
    def get_distance_between(self, from_code: str, to_code: str) -> Optional[Dict]:
        """Get distance between two airports"""
        return self._make_request('DISTANCE_BETWEEN', fromCode=from_code, toCode=to_code)
    
    # ==================== BATCH METHODS ====================
    
    def get_multiple_airports(self, codes: List[str]) -> Dict[str, Optional[Dict]]:
        """Get multiple airport info in parallel"""
        results = {}
        
        def fetch_one(code):
            return code, self.get_airport_info(code)
        
        with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS['airports']) as executor:
            futures = {executor.submit(fetch_one, code): code for code in codes}
            for future in as_completed(futures):
                code, data = future.result()
                results[code] = data
        
        return results
    
    def get_multiple_schedules(self, airport_directions: List[tuple]) -> Dict:
        """Get multiple airport schedules in parallel"""
        results = {}
        
        def fetch_one(params):
            code, direction = params
            return f"{code}_{direction}", self.get_airport_schedule(code, direction)
        
        with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS['flights']) as executor:
            futures = {executor.submit(fetch_one, params): params for params in airport_directions}
            for future in as_completed(futures):
                key, data = future.result()
                results[key] = data
        
        return results
    
    def get_multiple_aircraft(self, registrations: List[str]) -> Dict[str, Optional[Dict]]:
        """Get multiple aircraft info in parallel"""
        results = {}
        
        def fetch_one(reg):
            return reg, self.get_aircraft_info(reg)
        
        # Process in batches to avoid overwhelming API
        batch_size = PARALLEL_WORKERS['aircraft']
        for i in range(0, len(registrations), batch_size):
            batch = registrations[i:i+batch_size]
            
            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                futures = {executor.submit(fetch_one, reg): reg for reg in batch}
                for future in as_completed(futures):
                    reg, data = future.result()
                    results[reg] = data
            
            # Small delay between batches
            if i + batch_size < len(registrations):
                time.sleep(0.5)
        
        return results
    
    def get_multiple_delays(self, codes: List[str]) -> Dict[str, Optional[Dict]]:
        """Get delay stats for multiple airports"""
        results = {}
        
        def fetch_one(code):
            return code, self.get_airport_delays(code)
        
        with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS['delays']) as executor:
            futures = {executor.submit(fetch_one, code): code for code in codes}
            for future in as_completed(futures):
                code, data = future.result()
                results[code] = data
        
        return results
    
    def clear_cache(self):
        """Clear all cached data"""
        self.cache.clear()
        logger.info("Cache cleared")