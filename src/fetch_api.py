# src/fetch_api.py
import os
import requests
from time import sleep

API_HOST = "aerodatabox.p.rapidapi.com"
API_KEY = os.environ.get("AERODATABOX_API_KEY")

API_BASE = f"https://{API_HOST}"

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST
}

def safe_get(url, params=None, retries=3, backoff=2):
    for i in range(retries):
        r = requests.get(url, headers=HEADERS, params=params, timeout=20)
        if r.status_code == 200:
            return r.json()
        # for rate-limit or transient errors, retry
        sleep(backoff * (i + 1))
    r.raise_for_status()

def fetch_airport_by_iata(iata_code: str):
    """Fetch airport metadata by IATA."""
    url = f"{API_BASE}/airports/iata/{iata_code}"
    return safe_get(url)

def fetch_flights_for_airport(iata_code: str, direction="arrivals", days=1, limit=200):
    """
    Fetch flights for an airport.
    Note: endpoint shape may differ; this is a commonly used AeroDataBox path.
    Adjust if API returns different structure.
    """
    # Example endpoint - adjust if your plan / API differs
    url = f"{API_BASE}/flights/{direction}/iata/{iata_code}"
    params = {"limit": limit, "withLeg": "true", "hours": 24 * days}
    return safe_get(url, params=params)
