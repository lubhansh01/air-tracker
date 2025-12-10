"""
Configuration for 15 airports (national and international)
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ==================== API CONFIGURATION ====================
API_KEY = os.getenv('AERODATABOX_API_KEY')
API_HOST = os.getenv('AERODATABOX_API_HOST', 'aerodatabox.p.rapidapi.com')

# ==================== AIRPORT SELECTION (15 Airports) ====================
# Mix of National (Indian) and International airports
AIRPORT_CODES = [
    # National (India) - 7 airports
    'DEL',  # Delhi - Indira Gandhi International
    'BOM',  # Mumbai - Chhatrapati Shivaji Maharaj
    'MAA',  # Chennai
    'BLR',  # Bengaluru
    'HYD',  # Hyderabad
    'CCU',  # Kolkata
    'AMD',  # Ahmedabad
    
    # International - 8 airports
    'LHR',  # London Heathrow
    'JFK',  # New York JFK
    'DXB',  # Dubai
    'SIN',  # Singapore Changi
    'CDG',  # Paris Charles de Gaulle
    'FRA',  # Frankfurt
    'SYD',  # Sydney
    'NRT',  # Tokyo Narita
]

# Group airports by region for better organization
AIRPORT_GROUPS = {
    'India': ['DEL', 'BOM', 'MAA', 'BLR', 'HYD', 'CCU', 'AMD'],
    'Europe': ['LHR', 'CDG', 'FRA'],
    'North America': ['JFK'],
    'Middle East': ['DXB'],
    'Asia Pacific': ['SIN', 'SYD', 'NRT']
}

# Airport full names for display
AIRPORT_NAMES = {
    'DEL': 'Delhi (IGI)',
    'BOM': 'Mumbai (CSM)',
    'MAA': 'Chennai',
    'BLR': 'Bengaluru',
    'HYD': 'Hyderabad',
    'CCU': 'Kolkata',
    'AMD': 'Ahmedabad',
    'LHR': 'London Heathrow',
    'JFK': 'New York JFK',
    'DXB': 'Dubai',
    'SIN': 'Singapore',
    'CDG': 'Paris CDG',
    'FRA': 'Frankfurt',
    'SYD': 'Sydney',
    'NRT': 'Tokyo Narita'
}

# ==================== FETCHING STRATEGY ====================
# How many airports to process in parallel
PARALLEL_LIMITS = {
    'airport_info': 5,      # Airport details fetch
    'flight_schedules': 3,  # Flight schedules fetch
    'delay_stats': 2        # Delay statistics fetch
}

# Cache settings
CACHE_TTL = 300  # 5 minutes cache