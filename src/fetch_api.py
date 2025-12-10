# src/fetch_api.py
import os, requests, time
API_HOST = "aerodatabox.p.rapidapi.com"
API_KEY = os.environ.get("AERODATABOX_API_KEY")
HEADERS = {"x-rapidapi-key": API_KEY, "x-rapidapi-host": API_HOST}
BASE = f"https://{API_HOST}"

def safe_get(url, params=None, retries=3, backoff=2):
    for i in range(retries):
        r = requests.get(url, headers=HEADERS, params=params, timeout=20)
        if r.status_code == 200:
            return r.json()
        time.sleep(backoff*(i+1))
    r.raise_for_status()

def fetch_airport_by_iata(iata):
    return safe_get(f"{BASE}/airports/iata/{iata}")

def fetch_flights_for_airport(iata, direction="arrivals", days=1, limit=200):
    url = f"{BASE}/flights/{direction}/iata/{iata}"
    params = {"limit": limit, "hours": 24*days}
    return safe_get(url, params=params)
