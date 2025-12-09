import sqlite3
from pathlib import Path
from config import DB_PATH
from pkgutil import get_data

def init_db():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    sql = open(Path(__file__).resolve().parent.parent / "sql" / "create_tables.sql").read()
    cur.executescript(sql)
    conn.commit()
    conn.close()

def get_conn():
    return sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
