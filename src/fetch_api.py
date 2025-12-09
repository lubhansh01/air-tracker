import requests
from config import AERODATABOX_API_HOST, HEADERS
from time import sleep

API_BASE = f"https://{AERODATABOX_API_HOST}"

def fetch_airport_by_iata(iata_code):
    url = f"{API_BASE}/airports/iata/{iata_code}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_flights_for_airport(iata_code, direction="arrivals", days=3):
    # Example endpoint path; check actual AeroDataBox docs for exact URL patterns
    # This is a template — adjust path/params per API docs
    url = f"{API_BASE}/flights/{direction}/iata/{iata_code}"
    params = {"limit": 200, "days": days}
    r = requests.get(url, headers=HEADERS, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

# wrap with safe call and retry
def safe_get(fn, *args, retries=3, backoff=2, **kwargs):
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except requests.HTTPError as e:
            if i == retries - 1:
                raise
            sleep(backoff * (i + 1))
