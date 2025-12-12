import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # API Configuration
    API_HOST = "aerodatabox.p.rapidapi.com"
    API_KEY = os.getenv("AERODATABOX_API_KEY", "your_api_key_here")
    
    # Selected airports (15 major airports)
    AIRPORT_CODES = [
        'DEL', 'BOM', 'MAA', 'BLR', 'HYD',  # Indian airports
        'JFK', 'LAX', 'ORD', 'DFW', 'ATL',   # US airports
        'LHR', 'CDG', 'FRA', 'AMS', 'DXB',   # International hubs
        'SIN'  # Singapore
    ]
    
    # Time periods for data collection
    DAYS_BACK = 7
    
    HEADERS = {
        'x-rapidapi-key': API_KEY,
        'x-rapidapi-host': API_HOST
    }
    
    # API Endpoints
    ENDPOINTS = {
        'airport_info': 'https://aerodatabox.p.rapidapi.com/airports/icao/{icao_code}',
        'airport_data': 'https://aerodatabox.p.rapidapi.com/airports/iata/{iata_code}',
        'airport_stats': 'https://aerodatabox.p.rapidapi.com/airports/iata/{iata_code}/stats/routes/daily',
        'flights_by_airport': 'https://aerodatabox.p.rapidapi.com/flights/airports/iata/{iata_code}/{direction}/{date}',
        'aircraft_info': 'https://aerodatabox.p.rapidapi.com/aircrafts/reg/{registration}',
        'airport_delays': 'https://aerodatabox.p.rapidapi.com/airports/iata/{iata_code}/delays'
    }