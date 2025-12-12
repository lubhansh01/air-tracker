# robust_airport_insert.py  <- paste this into data_ingest.py or utils.py
from db import SessionLocal, Airport
from sqlalchemy.exc import IntegrityError
import time

def _safe_scalar_from_field(field, prefer="name"):
    """
    Accepts a field that may be:
      - a string (returns it)
      - a dict with 'name' / 'code' (returns field[prefer] if exists, else first string value found)
      - None (returns None)
    """
    if field is None:
        return None
    if isinstance(field, str):
        return field
    if isinstance(field, dict):
        # prefer 'name' (or 'code' if prefer='code'), else pick any string-like value
        if prefer in field and isinstance(field[prefer], (str, int, float)):
            return str(field[prefer])
        # try common keys
        for k in ("name", "code", "country", "continent"):
            if k in field and isinstance(field[k], (str, int, float)):
                return str(field[k])
        # fallback: convert to string representation (not ideal)
        return str(field)
    # fallback to string of value (numbers etc.)
    return str(field)

def fetch_and_store_airports(iata_list, get_airport_by_iata):
    session = SessionLocal()
    for iata in iata_list:
        try:
            data = get_airport_by_iata(iata)
            # Extract values safely (handles dict or string)
            country_val = _safe_scalar_from_field(data.get("country"))
            continent_val = _safe_scalar_from_field(data.get("continent"))
            name = data.get("name") or _safe_scalar_from_field(data.get("name"))
            city = data.get("city") or _safe_scalar_from_field(data.get("city"))
            latitude = None
            longitude = None
            loc = data.get("location") or {}
            if isinstance(loc, dict):
                latitude = loc.get("lat")
                longitude = loc.get("lon")

            airport = Airport(
                icao_code = data.get("icao"),
                iata_code = data.get("iata"),
                name = name,
                city = city,
                country = country_val,
                continent = continent_val,
                latitude = latitude,
                longitude = longitude,
                timezone = (data.get("timezone") or {}).get("tzName") if isinstance(data.get("timezone"), dict) else data.get("timezone")
            )

            # commit per-row so one bad row won't break the whole loop
            try:
                session.add(airport)
                session.commit()
                print(f"Inserted airport {iata}")
            except IntegrityError as ie:
                session.rollback()
                print(f"IntegrityError inserting {iata}: {ie}. Skipping.")
            except Exception as e:
                session.rollback()
                print(f"Error inserting {iata}: {e}. Skipping.")
            time.sleep(0.2)

        except Exception as e:
            # If the API call or parsing fails, log and continue to next airport
            print(f"Error fetching {iata}: {e}")
            # ensure transaction state clean
            try:
                session.rollback()
            except:
                pass
    session.close()
