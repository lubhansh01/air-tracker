-- 1) total number of flights for each aircraft model

SELECT a.model, COUNT(*) AS flights_count
FROM flights f
JOIN aircraft a ON f.aircraft_registration = a.registration
GROUP BY a.model
ORDER BY flights_count DESC;
