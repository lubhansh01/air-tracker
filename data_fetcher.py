def _store_airport(self, code: str, data: Dict) -> bool:
    """Store airport in database - SIMPLIFIED VERSION"""
    try:
        query = '''
        INSERT OR REPLACE INTO airport 
        (icao_code, iata_code, name, city, country, continent, 
         latitude, longitude, timezone)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = (
            str(data.get('icao', code)),
            str(data.get('iata', code)),
            str(data.get('name', AIRPORT_NAMES.get(code, code))),
            str(data.get('municipalityName', '')),
            str(data.get('country', {}).get('name', '')),
            str(data.get('continent', '')),
            float(data.get('location', {}).get('lat', 0)),
            float(data.get('location', {}).get('lon', 0)),
            str(data.get('timeZone', ''))
        )
        
        self.db.execute_query(query, params)
        return True
        
    except Exception as e:
        print(f"❌ Airport store error {code}: {e}")
        return False