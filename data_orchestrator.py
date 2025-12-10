"""
Intelligent data orchestrator that coordinates fetching from multiple endpoints
optimally based on the Swagger documentation structure.
"""
import time
from typing import Dict, List, Set, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import logging
from config import AIRPORT_CODES, DATA_LIMITS, PARALLEL_WORKERS
from swagger_clients import AeroDataBoxClient
from database import FlightDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataOrchestrator:
    """
    Orchestrates data fetching from multiple endpoints efficiently.
    Uses parallel processing and intelligent batching.
    """
    
    def __init__(self, db: FlightDatabase):
        self.client = AeroDataBoxClient()
        self.db = db
        self.stats = {
            'total_requests': 0,
            'cache_hits': 0,
            'errors': 0,
            'last_fetch': None
        }
    
    def fetch_all_data_strategically(self) -> Dict:
        """
        Main orchestration method - fetches data using optimal strategy
        
        Strategy:
        1. Fetch airport basics in parallel
        2. Fetch schedules for airports in parallel
        3. Extract unique aircraft and fetch their details
        4. Fetch delays for subset of airports
        5. Store everything in database
        """
        logger.info("🚀 Starting strategic data fetch...")
        start_time = time.time()
        
        # Step 1: Select airports to process (respect limits)
        airports_to_process = AIRPORT_CODES[:DATA_LIMITS['max_airports']]
        logger.info(f"Processing {len(airports_to_process)} airports")
        
        # Step 2: Fetch airport info in parallel
        logger.info("🌎 Fetching airport information...")
        airports_data = self.client.get_multiple_airports(airports_to_process)
        
        # Step 3: Prepare schedule requests
        schedule_requests = []
        for code in airports_to_process:
            schedule_requests.append((code, "departures"))
            if len(schedule_requests) < 4:  # Limit arrivals to first 4 airports
                schedule_requests.append((code, "arrivals"))
        
        # Step 4: Fetch schedules in parallel
        logger.info("✈️ Fetching flight schedules...")
        schedules_data = self.client.get_multiple_schedules(schedule_requests)
        
        # Step 5: Extract and fetch aircraft data
        unique_aircraft = self._extract_aircraft_from_schedules(schedules_data)
        logger.info(f"🛩️ Found {len(unique_aircraft)} unique aircraft")
        
        aircraft_data = {}
        if unique_aircraft:
            aircraft_to_fetch = list(unique_aircraft)[:DATA_LIMITS['max_aircraft']]
            logger.info(f"Fetching {len(aircraft_to_fetch)} aircraft details...")
            aircraft_data = self.client.get_multiple_aircraft(aircraft_to_fetch)
        
        # Step 6: Fetch delays for major airports only
        delay_airports = airports_to_process[:3]  # First 3 are usually major hubs
        logger.info(f"⏱️ Fetching delay stats for {delay_airports}...")
        delays_data = self.client.get_multiple_delays(delay_airports)
        
        # Step 7: Process and store all data
        logger.info("💾 Processing and storing data...")
        processing_results = self._process_and_store_all(
            airports_data, schedules_data, aircraft_data, delays_data
        )
        
        # Update stats
        elapsed = time.time() - start_time
        self.stats['last_fetch'] = datetime.now()
        self.stats['total_requests'] = len(airports_to_process) + len(schedule_requests) + len(aircraft_data) + len(delay_airports)
        
        logger.info(f"✅ Strategic fetch completed in {elapsed:.1f} seconds")
        logger.info(f"📊 Stats: {self.stats['total_requests']} requests, {processing_results['flights_stored']} flights stored")
        
        return {
            'success': True,
            'time_seconds': elapsed,
            'airports_processed': len(airports_to_process),
            'flights_stored': processing_results['flights_stored'],
            'aircraft_stored': processing_results['aircraft_stored'],
            'delays_stored': processing_results['delays_stored']
        }
    
    def _extract_aircraft_from_schedules(self, schedules_data: Dict) -> Set[str]:
        """Extract unique aircraft registrations from schedule data"""
        aircraft_set = set()
        
        for schedule_key, schedule in schedules_data.items():
            if not schedule or 'data' not in schedule:
                continue
            
            for flight in schedule['data'][:DATA_LIMITS['max_flights_per_airport']]:
                aircraft = flight.get('aircraft', {})
                reg = aircraft.get('reg')
                if reg and reg not in ['', 'N/A', None]:
                    aircraft_set.add(reg)
        
        return aircraft_set
    
    def _process_and_store_all(self, airports_data: Dict, schedules_data: Dict,
                              aircraft_data: Dict, delays_data: Dict) -> Dict:
        """Process all fetched data and store in database"""
        results = {
            'airports_stored': 0,
            'flights_stored': 0,
            'aircraft_stored': 0,
            'delays_stored': 0
        }
        
        # Store airports
        for code, data in airports_data.items():
            if data:
                self._store_airport(code, data)
                results['airports_stored'] += 1
        
        # Store flights from schedules
        for schedule_key, schedule in schedules_data.items():
            if schedule and 'data' in schedule:
                code, direction = schedule_key.split('_')
                for flight in schedule['data'][:DATA_LIMITS['max_flights_per_airport']]:
                    self._store_flight(flight, direction)
                    results['flights_stored'] += 1
        
        # Store aircraft
        for reg, data in aircraft_data.items():
            if data:
                self._store_aircraft(reg, data)
                results['aircraft_stored'] += 1
        
        # Store delays
        for code, data in delays_data.items():
            if data:
                self._store_delays(code, data)
                results['delays_stored'] += 1
        
        return results
    
    def _store_airport(self, code: str, data: Dict):
        """Store airport data in database"""
        try:
            query = '''
            INSERT OR REPLACE INTO airport 
            (icao_code, iata_code, name, city, country, continent, latitude, longitude, timezone)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            # Map API response to our schema
            params = (
                data.get('icao', code),  # Use provided code as fallback
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
            logger.error(f"Error storing airport {code}: {e}")
    
    def _store_flight(self, flight_data: Dict, direction: str):
        """Store flight data in database"""
        try:
            flight_number = flight_data.get('number')
            if not flight_number:
                return
            
            # Generate unique flight ID
            flight_id = f"{flight_number}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # Get airline info
            airline = flight_data.get('airline', {})
            
            # Get times
            departure = flight_data.get('departure', {})
            arrival = flight_data.get('arrival', {})
            
            # Get aircraft registration
            aircraft_reg = flight_data.get('aircraft', {}).get('reg', '')
            
            query = '''
            INSERT OR REPLACE INTO flights 
            (flight_id, flight_number, aircraft_registration, origin_iata, destination_iata,
             scheduled_departure, actual_departure, scheduled_arrival, actual_arrival,
             status, airline_code, airline_name, flight_date, direction)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE('now'), ?)
            '''
            
            params = (
                flight_id,
                flight_number,
                aircraft_reg,
                departure.get('airport', {}).get('iata', ''),
                arrival.get('airport', {}).get('iata', ''),
                departure.get('scheduledTime', {}).get('local', ''),
                departure.get('actualTime', {}).get('local', ''),
                arrival.get('scheduledTime', {}).get('local', ''),
                arrival.get('actualTime', {}).get('local', ''),
                flight_data.get('status', 'scheduled'),
                airline.get('icao', ''),
                airline.get('name', ''),
                direction
            )
            
            self.db.execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error storing flight: {e}")
    
    def _store_aircraft(self, registration: str, data: Dict):
        """Store aircraft data in database"""
        try:
            query = '''
            INSERT OR REPLACE INTO aircraft 
            (registration, model, manufacturer, icao_type_code, owner)
            VALUES (?, ?, ?, ?, ?)
            '''
            
            params = (
                registration,
                data.get('model', {}).get('text', ''),
                data.get('manufacturer', {}).get('name', ''),
                data.get('icaoCode', ''),
                data.get('owner', {}).get('name', '')
            )
            
            self.db.execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error storing aircraft {registration}: {e}")
    
    def _store_delays(self, code: str, data: Dict):
        """Store delay statistics in database"""
        try:
            query = '''
            INSERT OR REPLACE INTO airport_delays 
            (airport_iata, delay_date, total_flights, delayed_flights, 
             avg_delay_min, median_delay_min, canceled_flights)
            VALUES (?, DATE('now'), ?, ?, ?, ?, ?)
            '''
            
            # Map delay statistics from API response
            stats = data.get('statistics', {})
            flights = stats.get('flights', {})
            delays = stats.get('delays', {})
            
            params = (
                code,
                flights.get('total', 0),
                flights.get('delayed', 0),
                delays.get('averageMinutes', 0),
                delays.get('medianMinutes', 0),
                flights.get('canceled', 0)
            )
            
            self.db.execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error storing delays for {code}: {e}")
    
    def quick_refresh(self) -> Dict:
        """
        Quick refresh - only fetch essential data
        Returns within 10-15 seconds
        """
        logger.info("⚡ Starting quick refresh...")
        start_time = time.time()
        
        # Only fetch delays and recent schedules
        quick_airports = AIRPORT_CODES[:3]  # Only 3 airports
        
        # Fetch delays
        delays_data = self.client.get_multiple_delays(quick_airports)
        
        # Fetch only departures for quick airports
        schedule_requests = [(code, "departures") for code in quick_airports]
        schedules_data = self.client.get_multiple_schedules(schedule_requests)
        
        # Store the data
        for code, delay_data in delays_data.items():
            if delay_data:
                self._store_delays(code, delay_data)
        
        flight_count = 0
        for schedule_key, schedule in schedules_data.items():
            if schedule and 'data' in schedule:
                code, direction = schedule_key.split('_')
                # Store only first 10 flights
                for flight in schedule['data'][:10]:
                    self._store_flight(flight, direction)
                    flight_count += 1
        
        elapsed = time.time() - start_time
        
        return {
            'success': True,
            'time_seconds': elapsed,
            'flights_updated': flight_count,
            'airports_updated': len(quick_airports),
            'message': f'Quick refresh completed in {elapsed:.1f}s'
        }
    
    def get_stats(self) -> Dict:
        """Get orchestrator statistics"""
        return self.stats.copy()