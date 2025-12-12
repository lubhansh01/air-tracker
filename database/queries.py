class FlightQueries:
    """Collection of SQL queries for flight analytics"""
    
    # 1) Show the total number of flights for each aircraft model
    QUERY_1 = """
        SELECT 
            a.model,
            COUNT(f.flight_id) as flight_count
        FROM aircraft a
        LEFT JOIN flights f ON a.registration = f.aircraft_registration
        WHERE a.model IS NOT NULL
        GROUP BY a.model
        ORDER BY flight_count DESC
    """
    
    # 2) List all aircraft (registration, model) that have been assigned to more than 5 flights
    QUERY_2 = """
        SELECT 
            a.registration,
            a.model,
            COUNT(f.flight_id) as flight_count
        FROM aircraft a
        JOIN flights f ON a.registration = f.aircraft_registration
        GROUP BY a.registration, a.model
        HAVING COUNT(f.flight_id) > 5
        ORDER BY flight_count DESC
    """
    
    # 3) For each airport, display its name and the number of outbound flights
    QUERY_3 = """
        SELECT 
            ap.name as airport_name,
            COUNT(f.flight_id) as outbound_flights
        FROM airport ap
        LEFT JOIN flights f ON ap.iata_code = f.origin_iata
        GROUP BY ap.iata_code, ap.name
        HAVING COUNT(f.flight_id) > 5
        ORDER BY outbound_flights DESC
    """
    
    # 4) Find the top 3 destination airports (name, city) by number of arriving flights
    QUERY_4 = """
        SELECT 
            ap.name as airport_name,
            ap.city,
            COUNT(f.flight_id) as arriving_flights
        FROM airport ap
        JOIN flights f ON ap.iata_code = f.destination_iata
        GROUP BY ap.iata_code, ap.name, ap.city
        ORDER BY arriving_flights DESC
        LIMIT 3
    """
    
    # 5) Show for each flight: number, origin, destination, and domestic/international label
    QUERY_5 = """
        SELECT 
            f.flight_number,
            o.country as origin_country,
            d.country as destination_country,
            CASE 
                WHEN o.country = d.country THEN 'Domestic'
                ELSE 'International'
            END as flight_type
        FROM flights f
        JOIN airport o ON f.origin_iata = o.iata_code
        JOIN airport d ON f.destination_iata = d.iata_code
        WHERE o.country IS NOT NULL AND d.country IS NOT NULL
        LIMIT 100
    """
    
    # 6) Show the 5 most recent arrivals at "DEL" airport
    QUERY_6 = """
        SELECT 
            f.flight_number,
            f.aircraft_registration,
            o.name as departure_airport,
            f.actual_arrival,
            f.status
        FROM flights f
        JOIN airport o ON f.origin_iata = o.iata_code
        WHERE f.destination_iata = 'DEL'
        ORDER BY f.actual_arrival DESC
        LIMIT 5
    """
    
    # 7) Find all airports with no arriving flights
    QUERY_7 = """
        SELECT 
            ap.iata_code,
            ap.name,
            ap.city,
            ap.country
        FROM airport ap
        LEFT JOIN flights f ON ap.iata_code = f.destination_iata
        WHERE f.flight_id IS NULL
    """
    
    # 8) For each airline, count the number of flights by status
    QUERY_8 = """
        SELECT 
            airline_code,
            SUM(CASE WHEN status = 'On Time' THEN 1 ELSE 0 END) as on_time,
            SUM(CASE WHEN status = 'Delayed' THEN 1 ELSE 0 END) as delayed,
            SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN status = 'Scheduled' THEN 1 ELSE 0 END) as scheduled,
            COUNT(*) as total_flights
        FROM flights
        WHERE airline_code IS NOT NULL AND airline_code != ''
        GROUP BY airline_code
        ORDER BY total_flights DESC
    """
    
    # 9) Show all cancelled flights
    QUERY_9 = """
        SELECT 
            f.flight_number,
            f.aircraft_registration,
            a.model as aircraft_model,
            o.name as origin_airport,
            d.name as destination_airport,
            f.scheduled_departure,
            f.status
        FROM flights f
        LEFT JOIN aircraft a ON f.aircraft_registration = a.registration
        LEFT JOIN airport o ON f.origin_iata = o.iata_code
        LEFT JOIN airport d ON f.destination_iata = d.iata_code
        WHERE f.status = 'Cancelled'
        ORDER BY f.scheduled_departure DESC
    """
    
    # 10) List city pairs with more than 2 different aircraft models
    QUERY_10 = """
        SELECT 
            o.city as origin_city,
            d.city as destination_city,
            COUNT(DISTINCT ac.model) as unique_aircraft_models,
            COUNT(f.flight_id) as total_flights
        FROM flights f
        JOIN airport o ON f.origin_iata = o.iata_code
        JOIN airport d ON f.destination_iata = d.iata_code
        JOIN aircraft ac ON f.aircraft_registration = ac.registration
        GROUP BY o.city, d.city
        HAVING COUNT(DISTINCT ac.model) > 2
        ORDER BY unique_aircraft_models DESC
    """
    
    # 11) For each destination airport, compute % of delayed flights
    QUERY_11 = """
        SELECT 
            ap.name as airport_name,
            ap.city,
            COUNT(f.flight_id) as total_arrivals,
            SUM(CASE WHEN f.status = 'Delayed' THEN 1 ELSE 0 END) as delayed_arrivals,
            ROUND(
                (SUM(CASE WHEN f.status = 'Delayed' THEN 1 ELSE 0 END) * 100.0 / 
                NULLIF(COUNT(f.flight_id), 0)), 2
            ) as delay_percentage
        FROM airport ap
        LEFT JOIN flights f ON ap.iata_code = f.destination_iata
        WHERE f.flight_id IS NOT NULL
        GROUP BY ap.iata_code, ap.name, ap.city
        HAVING COUNT(f.flight_id) > 0
        ORDER BY delay_percentage DESC
    """
    
    # Additional useful queries
    @staticmethod
    def get_flights_by_date_range(start_date, end_date):
        return f"""
            SELECT * FROM flights 
            WHERE flight_date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY scheduled_departure
        """
    
    @staticmethod
    def get_airport_summary():
        return """
            SELECT 
                COUNT(DISTINCT iata_code) as total_airports,
                COUNT(DISTINCT country) as countries_covered,
                GROUP_CONCAT(DISTINCT continent) as continents
            FROM airport
        """
    
    @staticmethod
    def get_flight_summary():
        return """
            SELECT 
                COUNT(*) as total_flights,
                COUNT(DISTINCT airline_code) as airlines,
                COUNT(DISTINCT aircraft_registration) as unique_aircraft,
                AVG(CASE WHEN status = 'Delayed' THEN 1 ELSE 0 END) * 100 as avg_delay_percentage
            FROM flights
        """