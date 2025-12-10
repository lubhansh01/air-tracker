# src/db.py
import sqlite3
from pathlib import Path
from config import DB_PATH  # uses the config you already have

def init_db():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    sql = open(Path(__file__).resolve().parent.parent / "sql" / "create_tables.sql").read()
    conn.executescript(sql)
    conn.commit()
    conn.close()

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)
