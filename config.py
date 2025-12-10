import os
from dotenv import load_dotenv
from typing import List, Dict

load_dotenv()

# ==================== API CONFIGURATION ====================
API_KEY = os.getenv('AERODATABOX_API_KEY')
API_HOST = os.getenv('AERODATABOX_API_HOST', 'aerodatabox.p.rapidapi.com')

HEADERS = {
    'X-RapidAPI-Key': API_KEY,
    'X-RapidAPI-Host': API_HOST
}

BASE_URL = f"https://{API_HOST}"

# ==================== ENDPOINT DEFINITIONS (From Swagger) ====================
ENDPOINTS = {
    # Airport API (🌎)
    'AIRPORT_INFO': '/airports/{code}',  # 🌎 Get Airport
    'AIRPORT_RUNWAYS': '/airports/{code}/runways',
    'SEARCH_AIRPORTS_LOCATION': '/airports/search/location',
    'SEARCH_AIRPORTS_IP': '/airports/search/ip',
    'SEARCH_AIRPORTS_TEXT': '/airports/search/text',
    
    # Flights API (🗓️)
    'AIRPORT_SCHEDULE': '/flights/airports/{code}/{direction}',  # 🗓️ FIDS & Schedules
    'FLIGHT_STATUS': '/flights/{flightNumber}/{date}',  # 🗓️ Flight Status
    'FLIGHT_HISTORY': '/flights/{identifier}/range/{fromDate}/{toDate}',  # 🗓️ Flight History & Schedule
    'FLIGHT_DATES': '/flights/{identifier}/dates',  # 🗓️ Flight Departure/Arrival Dates
    'SEARCH_FLIGHTS': '/flights/search/term',  # 🗓️ Search Flight Numbers
    
    # Aircraft API (✈️)
    'AIRCRAFT_INFO': '/aircraft/{registration}',  # ✈️ Get aircraft
    'AIRCRAFT_REGISTRATION_HISTORY': '/aircraft/{identifier}/registrations',
    'AIRLINE_FLEET': '/airlines/{icao}/fleet',
    'AIRCRAFT_PHOTO': '/aircraft/{registration}/photo',
    'SEARCH_AIRCRAFT': '/aircraft/search/term',
    
    # Statistical API (📊)
    'AIRPORT_DELAYS': '/airports/{code}/delays',  # 📊 Airport delays
    'AIRPORT_ROUTES': '/airports/{code}/statistics/routes/daily',
    'FLIGHT_DELAY_STATS': '/flights/{flightNumber}/statistics/delays',
    'GLOBAL_DELAYS': '/statistics/delays/global',
    
    # Miscellaneous API
    'AIRPORT_TIME': '/airports/{code}/time/local',
    'AIRPORT_SUN_TIMES': '/airports/{code}/time/sun',
    'DISTANCE_BETWEEN': '/airports/distance/{fromCode}/{toCode}',
    'FLIGHT_TIME': '/flights/time/{fromCode}/{toCode}',
    'AIRPORT_WEATHER': '/airports/{code}/weather',
    'COUNTRIES_LIST': '/countries'
}

# ==================== OPTIMIZATION SETTINGS ====================
# Selected airports (mix of major international airports)
AIRPORT_CODES = ['DEL', 'BOM', 'LHR', 'JFK', 'DXB', 'SIN', 'CDG', 'FRA']

# Parallel processing settings
PARALLEL_WORKERS = {
    'airports': 4,      # For airport info fetching
    'flights': 3,       # For flight schedule fetching
    'aircraft': 5,      # For aircraft info fetching
    'delays': 2         # For delay stats fetching
}

# Data limits for optimization
DATA_LIMITS = {
    'max_airports': 6,           # Max airports to process
    'max_flights_per_airport': 25, # Max flights per airport
    'max_aircraft': 15,          # Max aircraft details to fetch
    'cache_duration_seconds': 300, # Cache duration (5 minutes)
    'max_retries': 3,            # Max retry attempts
    'retry_delay': 1,            # Seconds between retries
}

# API Tier awareness (from Swagger labels)
TIER_PRIORITY = {
    'low': ['AIRPORT_INFO', 'AIRPORT_TIME', 'COUNTRIES_LIST'],
    'medium': ['AIRPORT_SCHEDULE', 'AIRCRAFT_INFO', 'AIRPORT_DELAYS'],
    'high': ['FLIGHT_HISTORY', 'AIRPORT_ROUTES', 'FLIGHT_DELAY_STATS']
}

def get_endpoint_url(endpoint_name: str, **kwargs) -> str:
    """Build complete endpoint URL with parameters"""
    if endpoint_name not in ENDPOINTS:
        raise ValueError(f"Unknown endpoint: {endpoint_name}")
    
    endpoint = ENDPOINTS[endpoint_name]
    
    # Replace path parameters
    for key, value in kwargs.items():
        if f'{{{key}}}' in endpoint:
            endpoint = endpoint.replace(f'{{{key}}}', str(value))
    
    return BASE_URL + endpoint

def get_headers() -> Dict:
    """Get headers with optional additional headers"""
    return HEADERS.copy()