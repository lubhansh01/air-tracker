import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
API_KEY = os.getenv('AERODATABOX_API_KEY')
API_HOST = os.getenv('AERODATABOX_API_HOST')
HEADERS = {
    'x-rapidapi-key': API_KEY,
    'x-rapidapi-host': API_HOST
}

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'flight_analytics'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'port': os.getenv('DB_PORT', 3306)
}

# Sample Airports (10 airports from different regions)
AIRPORT_CODES = ['DEL', 'BOM', 'MAA', 'BLR', 'HYD', 'JFK', 'LHR', 'DXB', 'SIN', 'CDG']