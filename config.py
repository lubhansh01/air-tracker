"""
Configuration for AeroDataBox API
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ==================== API CONFIGURATION ====================
API_KEY = os.getenv('AERODATABOX_API_KEY')
API_HOST = os.getenv('AERODATABOX_API_HOST', 'aerodatabox.p.rapidapi.com')

BASE_URL = f"https://{API_HOST}"

HEADERS = {
    'X-RapidAPI-Key': API_KEY,
    'X-RapidAPI-Host': API_HOST
}

# ==================== ENDPOINTS CONFIGURATION ====================
ENDPOINTS = {
    # AIRPORT API (Tier 1 & 2)
    'AIRPORT_INFO': '/airports/{codeType}/{code}',
    'AIRPORT_RUNWAYS': '/airports/{codeType}/{code}/runways',
    'AIRPORT_SEARCH_LOCATION': '/airports/search/location',
    'AIRPORT_SEARCH_TEXT': '/airports/search/term',
    
    # AIRCRAFT API (Tier 1 & 2)
    'AIRCRAFT_INFO': '/aircrafts/{searchBy}/{searchParam}',
    'AIRCRAFT_REG_HISTORY': '/aircrafts/{searchBy}/{searchParam}/registrations',
    'SEARCH_AIRCRAFT': '/aircrafts/search/term',
    'AIRCRAFT_IMAGE': '/aircrafts/reg/{reg}/image/beta',
    
    # FLIGHT API (Tier 2)
    'FLIGHT_STATUS': '/flights/{searchBy}/{searchParam}',
    'FLIGHT_STATUS_DATE': '/flights/{searchBy}/{searchParam}/{dateLocal}',
    'AIRPORT_FIDS': '/flights/airports/{codeType}/{code}',
    'AIRPORT_FIDS_RANGE': '/flights/airports/{codeType}/{code}/{fromLocal}/{toLocal}',
    'SEARCH_FLIGHTS': '/flights/search/term',
    'FLIGHT_DATES': '/flights/{searchBy}/{searchParam}/dates',
    
    # STATISTICAL API (Tier 3)
    'AIRPORT_DELAYS': '/airports/{codeType}/{code}/delays',
    'GLOBAL_DELAYS': '/airports/delays',
    'AIRPORT_ROUTES': '/airports/{codeType}/{code}/stats/routes/daily',
    
    # MISCELLANEOUS API (Tier 1 & 2)
    'AIRPORT_TIME': '/airports/{codeType}/{code}/time/local',
    'AIRPORT_SOLAR_TIME': '/airports/{codeType}/{code}/time/solar',
    'DISTANCE_TIME': '/airports/{codeType}/{codeFrom}/distance-time/{codeTo}',
    'AIRPORT_WEATHER': '/airports/{codeType}/{code}/weather',
    'COUNTRIES_LIST': '/countries',
}

# ==================== APPLICATION SETTINGS ====================

AIRPORT_CODES = ['DEL', 'BOM', 'LHR', 'JFK', 'DXB', 'SIN']

# Code type mapping (IATA vs ICAO)
CODE_TYPES = {
    'iata': 'iata',
    'icao': 'icao'
}

# Data fetching strategy
FETCH_STRATEGY = {
    'timeout': 10,
    'max_retries': 2,
    'cache_ttl': 300,  # 5 minutes cache
    'parallel_workers': 3,
    'rate_limit_delay': 0.5
}

def build_url(endpoint_name, **params):
    """Build complete URL for an endpoint"""
    if endpoint_name not in ENDPOINTS:
        raise ValueError(f"Unknown endpoint: {endpoint_name}")
    
    endpoint = ENDPOINTS[endpoint_name]
    
    # Replace all parameters in the endpoint
    for key, value in params.items():
        placeholder = f'{{{key}}}'
        if placeholder in endpoint:
            endpoint = endpoint.replace(placeholder, str(value))
    
    return BASE_URL + endpoint

def get_code_type(code):
    """Determine if code is IATA (3 letters) or ICAO (4 letters)"""
    if len(code) == 3 and code.isalpha():
        return 'iata'
    elif len(code) == 4 and code.isalpha():
        return 'icao'
    else:
        return 'iata'  # Default to IATA