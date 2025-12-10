# src/fetch_api.py
import os
import requests
import time
from typing import Any, Optional, Dict

API_HOST = "aerodatabox.p.rapidapi.com"
API_KEY = os.environ.get("AERODATABOX_API_KEY")
HEADERS = {"x-rapidapi-key": API_KEY, "x-rapidapi-host": API_HOST}
BASE = f"https://{API_HOST}"

def safe_get(url: str, params: Optional[Dict] = None, retries: int = 3, backoff: int = 2) -> Optional[Any]:
    """
    GET with retries. Returns JSON on 200, None on 404, raises last exception on repeated failure.
    """
    last_exc = None
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                return None
            # for other status codes, prepare to retry
            last_exc = RuntimeError("HTTP {} for {}: {}".format(r.status_code, url, r.text[:200]))
        except Exception as e:
            last_exc = e
        time.sleep(backoff * (i + 1))
    # After retries, raise the last exception so caller can log/fail
    raise last_exc

def fetch_airport_by_iata(iata_code: str) -> Optional[Any]:
    url = "{}/airports/iata/{}".format(BASE, iata_code)
    return safe_get(url)

def fetch_flights_for_airport(iata_code: str, direction: str = "arrivals", days: int = 1, limit: int = 200) -> Optional[Any]:
    """
    Try the main flights endpoint, fall back to an alternate pattern if available.
    Returns JSON (list/dict) on success or None when endpoint not found / no data.
    """
    params = {"limit": limit, "hours": 24 * days}
    # primary pattern
    url1 = "{}/flights/{}/iata/{}".format(BASE, direction, iata_code)
    data = safe_get(url1, params=params)
    if data is not None:
        return data

    # alternate pattern (some AeroDataBox plans/vendors use different endpoints)
    url2 = "{}/flights/airport/{}/{}".format(BASE, iata_code, direction)
    try:
        return safe_get(url2, params=params)
    except Exception:
        return None