-- queries.sql

-- 1) Total number of flights for each aircraft model
SELECT a.model, COUNT(f.flight_id) AS cnt
FROM flights f
LEFT JOIN aircraft a ON f.aircraft_registration = a.registration
GROUP BY a.model
ORDER BY cnt DESC;

-- 2) Aircraft (registration, model) assigned to more than 5 flights
SELECT a.registration, a.model, COUNT(f.flight_id) AS cnt
FROM flights f
JOIN aircraft a ON f.aircraft_registration = a.registration
GROUP BY a.registration, a.model
HAVING cnt > 5;

-- 3) For each airport, name and number of outbound flights (only >5 flights)
SELECT ap.name, COUNT(f.flight_id) AS outbound_count
FROM flights f
JOIN airport ap ON ap.iata_code = f.origin_iata
GROUP BY ap.name
HAVING outbound_count > 5
ORDER BY outbound_count DESC;

-- 4) Top 3 destination airports by arriving flights
SELECT ap.name, ap.city, COUNT(f.flight_id) AS arrivals
FROM flights f
JOIN airport ap ON ap.iata_code = f.destination_iata
GROUP BY ap.name, ap.city
ORDER BY arrivals DESC
LIMIT 3;

-- 5) Show for each flight: number, origin, destination, and label Domestic/International
SELECT f.flight_number, f.origin_iata, f.destination_iata,
CASE WHEN o.country = d.country THEN 'Domestic' ELSE 'International' END AS route_type
FROM flights f
LEFT JOIN airport o ON o.iata_code = f.origin_iata
LEFT JOIN airport d ON d.iata_code = f.destination_iata;

-- 6) 5 most recent arrivals at “DEL”
SELECT f.flight_number, f.aircraft_registration, o.name AS departure_airport, f.actual_arrival
FROM flights f
LEFT JOIN airport o ON o.iata_code = f.origin_iata
WHERE f.destination_iata = 'DEL'
ORDER BY f.actual_arrival DESC
LIMIT 5;

-- 7) Airports with no arriving flights
SELECT ap.name, ap.iata_code
FROM airport ap
LEFT JOIN flights f ON ap.iata_code = f.destination_iata
WHERE f.flight_id IS NULL;

-- 8) For each airline, count number of flights by status
SELECT airline_code,
SUM(CASE WHEN status = 'On Time' THEN 1 ELSE 0 END) AS on_time,
SUM(CASE WHEN status = 'Delayed' THEN 1 ELSE 0 END) AS delayed,
SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled,
COUNT(*) AS total
FROM flights
GROUP BY airline_code;

-- 9) All cancelled flights with aircraft and both airports ordered by departure time desc
SELECT f.flight_number, f.aircraft_registration, o.name AS origin, d.name AS destination, f.scheduled_departure
FROM flights f
LEFT JOIN airport o ON o.iata_code = f.origin_iata
LEFT JOIN airport d ON d.iata_code = f.destination_iata
WHERE f.status = 'Cancelled'
ORDER BY f.scheduled_departure DESC;

-- 10) City pairs with more than 2 different aircraft models operating
SELECT o.city || '-' || d.city AS route, COUNT(DISTINCT a.model) AS models_count
FROM flights f
JOIN airport o ON o.iata_code = f.origin_iata
JOIN airport d ON d.iata_code = f.destination_iata
JOIN aircraft a ON a.registration = f.aircraft_registration
GROUP BY o.city, d.city
HAVING models_count > 2;

-- 11) For each destination compute % delayed among arrivals
SELECT ap.name, ap.iata_code,
SUM(CASE WHEN f.status = 'Delayed' THEN 1 ELSE 0 END) * 100.0 / COUNT(f.flight_id) AS pct_delayed
FROM flights f
JOIN airport ap ON ap.iata_code = f.destination_iata
GROUP BY ap.name, ap.iata_code
ORDER BY pct_delayed DESC;
