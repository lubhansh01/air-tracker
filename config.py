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
    
    # AIRCRAFT API (Tier 1 & 2)
    'AIRCRAFT_INFO': '/aircrafts/{searchBy}/{searchParam}',
    
    # FLIGHT API (Tier 2)
    'FLIGHT_STATUS': '/flights/{searchBy}/{searchParam}',
    'FLIGHT_STATUS_DATE': '/flights/{searchBy}/{searchParam}/{dateLocal}',
    'AIRPORT_FIDS': '/flights/airports/{codeType}/{code}',
    'SEARCH_FLIGHTS': '/flights/search/term',
    
    # STATISTICAL API (Tier 3)
    'AIRPORT_DELAYS': '/airports/{codeType}/{code}/delays',
    'GLOBAL_DELAYS': '/airports/delays',
    'AIRPORT_ROUTES': '/airports/{codeType}/{code}/stats/routes/daily',
}

# ==================== APPLICATION SETTINGS ====================
AIRPORT_CODES = ['DEL', 'BOM', 'LHR', 'JFK', 'DXB', 'SIN']

def get_code_type(code):
    """Determine if code is IATA (3 letters) or ICAO (4 letters)"""
    if len(code) == 3 and code.isalpha():
        return 'iata'
    elif len(code) == 4 and code.isalpha():
        return 'icao'
    else:
        return 'iata'  # Default to IATA

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