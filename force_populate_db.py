# force_populate_db.py
from dotenv import load_dotenv
load_dotenv()
from sqlalchemy import create_engine, text
import os
DB_URL = os.getenv("DATABASE_URL","sqlite:///flight_analytics.db")
print("Using DB URL:", DB_URL)
engine = create_engine(DB_URL, future=True)
with engine.connect() as conn:
    conn.execute(text("INSERT OR IGNORE INTO airport (icao_code,iata_code,name,city,country,continent,latitude,longitude,timezone) VALUES ('VIDP','DEL','Indira Gandhi Intl','New Delhi','India','Asia',28.5665,77.1031,'Asia/Kolkata')"))
    conn.execute(text("INSERT OR IGNORE INTO airport (icao_code,iata_code,name,city,country,continent,latitude,longitude,timezone) VALUES ('VABB','BOM','Chhatrapati Shivaji Intl','Mumbai','India','Asia',19.0896,72.8656,'Asia/Kolkata')"))
    conn.execute(text("INSERT OR IGNORE INTO airport (icao_code,iata_code,name,city,country,continent,latitude,longitude,timezone) VALUES ('VOBL','BLR','Kempegowda Intl','Bengaluru','India','Asia',13.1986,77.7066,'Asia/Kolkata')"))
    conn.execute(text("INSERT OR IGNORE INTO aircraft (registration,model,manufacturer,icao_type_code,owner) VALUES ('VT-ABC','A320','Airbus','A320','DemoAir')"))
    conn.execute(text("INSERT OR IGNORE INTO aircraft (registration,model,manufacturer,icao_type_code,owner) VALUES ('VT-XYZ','B737','Boeing','B737','DemoAir')"))
    conn.execute(text(\"\"\"INSERT OR IGNORE INTO flights (flight_id,flight_number,aircraft_registration,origin_iata,destination_iata,scheduled_departure,actual_departure,scheduled_arrival,actual_arrival,status,airline_code) VALUES ('DEMO1','AI101','VT-ABC','DEL','BOM','2025-12-12T08:00:00Z','2025-12-12T08:05:00Z','2025-12-12T10:00:00Z','2025-12-12T10:10:00Z','Delayed','AI')\"\"\"))
    conn.execute(text(\"\"\"INSERT OR IGNORE INTO flights (flight_id,flight_number,aircraft_registration,origin_iata,destination_iata,scheduled_departure,actual_departure,scheduled_arrival,actual_arrival,status,airline_code) VALUES ('DEMO2','6E202','VT-XYZ','BOM','BLR','2025-12-12T09:00:00Z','2025-12-12T09:00:00Z','2025-12-12T11:00:00Z','2025-12-12T10:55:00Z','On Time','6E')\"\"\"))
    conn.commit()
    print("Inserted demo dataset.")
    for t in ("airport","aircraft","flights"):
        print(t, "count ->", conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar_one())
