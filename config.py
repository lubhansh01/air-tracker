import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = os.environ.get("DB_PATH", str(BASE_DIR / "data" / "airtracker.db"))

AERODATABOX_API_HOST = "aerodatabox.p.rapidapi.com"
AERODATABOX_API_KEY = os.environ.get("AERODATABOX_API_KEY")  # set in env or in Streamlit secrets
HEADERS = {
    "x-rapidapi-key": AERODATABOX_API_KEY,
    "x-rapidapi-host": AERODATABOX_API_HOST
}