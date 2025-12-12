import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from api.config import Config

class AeroDataBoxAPI:
    def __init__(self):
        self.headers = Config.HEADERS
        self.endpoints = Config.ENDPOINTS
        
    def make_request(self, url, params=None):
        """Make API request with error handling"""
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API Request failed: {e}")
            return None
    
    def get_airport_info(self, iata_code):
        """Get airport information by IATA code"""
        url = self.endpoints['airport_data'].format(iata_code=iata_code)
        return self.make_request(url)
    
    def get_airport_delays(self, iata_code):
        """Get airport delay statistics"""
        url = self.endpoints['airport_delays'].format(iata_code=iata_code)
        return self.make_request(url)
    
    def get_flights_by_airport(self, iata_code, direction, date):
        """
        Get flights for specific airport and date
        direction: 'departures' or 'arrivals'
        date: YYYY-MM-DD format
        """
        url = self.endpoints['flights_by_airport'].format(
            iata_code=iata_code,
            direction=direction,
            date=date
        )
        return self.make_request(url)
    
    def get_aircraft_info(self, registration):
        """Get aircraft information by registration"""
        url = self.endpoints['aircraft_info'].format(registration=registration)
        return self.make_request(url)
    
    def get_airport_stats(self, iata_code, date_from, date_to):
        """Get airport statistics for date range"""
        url = self.endpoints['airport_stats'].format(iata_code=iata_code)
        params = {
            'dateFrom': date_from,
            'dateTo': date_to
        }
        return self.make_request(url, params)
    
    def get_flights_for_period(self, iata_code, days_back=7):
        """Get flights for past N days"""
        all_flights = []
        
        for i in range(days_back):
            date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            
            # Get departures
            departures = self.get_flights_by_airport(iata_code, 'departures', date)
            if departures and 'departures' in departures:
                for flight in departures['departures']:
                    flight['direction'] = 'departure'
                    flight['date'] = date
                    flight['airport'] = iata_code
                    all_flights.append(flight)
            
            # Get arrivals
            arrivals = self.get_flights_by_airport(iata_code, 'arrivals', date)
            if arrivals and 'arrivals' in arrivals:
                for flight in arrivals['arrivals']:
                    flight['direction'] = 'arrival'
                    flight['date'] = date
                    flight['airport'] = iata_code
                    all_flights.append(flight)
            
            time.sleep(0.5)  # Rate limiting
            
        return all_flights