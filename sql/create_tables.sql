-- airports table
CREATE TABLE IF NOT EXISTS airport (
  airport_id INTEGER PRIMARY KEY AUTOINCREMENT,
  icao_code TEXT UNIQUE,
  iata_code TEXT UNIQUE,
  name TEXT,
  city TEXT,
  country TEXT,
  continent TEXT,
  latitude REAL,
  longitude REAL,
  timezone TEXT
);

-- aircraft table
CREATE TABLE IF NOT EXISTS aircraft (
  aircraft_id INTEGER PRIMARY KEY AUTOINCREMENT,
  registration TEXT UNIQUE,
  model TEXT,
  manufacturer TEXT,
  icao_type_code TEXT,
  owner TEXT
);

-- flights table
CREATE TABLE IF NOT EXISTS flights (
  flight_id TEXT PRIMARY KEY,
  flight_number TEXT,
  aircraft_registration TEXT,
  origin_iata TEXT,
  destination_iata TEXT,
  scheduled_departure TEXT,
  actual_departure TEXT,
  scheduled_arrival TEXT,
  actual_arrival TEXT,
  status TEXT,
  airline_code TEXT
);

-- airport_delays table
CREATE TABLE IF NOT EXISTS airport_delays (
  delay_id INTEGER PRIMARY KEY AUTOINCREMENT,
  airport_iata TEXT,
  delay_date TEXT,
  total_flights INTEGER,
  delayed_flights INTEGER,
  avg_delay_min INTEGER,
  median_delay_min INTEGER,
  canceled_flights INTEGER
);
