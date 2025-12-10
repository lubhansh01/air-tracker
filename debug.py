"""
Debug script for Air Tracker
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

load_dotenv()

print("🔍 AIR TRACKER DEBUG SCRIPT")
print("=" * 60)

# Check 1: Environment
print("\n1️⃣ CHECKING ENVIRONMENT...")
api_key = os.getenv('AERODATABOX_API_KEY')
api_host = os.getenv('AERODATABOX_API_HOST')

if not api_key or api_key == 'your_actual_api_key_here':
    print("❌ ERROR: API key not set in .env file")
    print("   Please update .env with your actual AeroDataBox API key")
else:
    print(f"✅ API Key: {api_key[:10]}... (first 10 chars)")
    print(f"✅ API Host: {api_host}")

# Check 2: Database
print("\n2️⃣ CHECKING DATABASE...")
try:
    from database import FlightDatabase
    db = FlightDatabase()
    
    # Check tables
    tables = db.execute_query("SELECT name FROM sqlite_master WHERE type='table'")
    if tables:
        print(f"✅ Database tables: {[t[0] for t in tables]}")
        
        # Check row counts
        for table in ['airport', 'flights', 'aircraft', 'airport_delays']:
            count = db.execute_query(f"SELECT COUNT(*) FROM {table}")
            print(f"   {table}: {count[0][0] if count else 0} rows")
    else:
        print("❌ No tables found in database")
        
except Exception as e:
    print(f"❌ Database error: {e}")

# Check 3: API Connection
print("\n3️⃣ TESTING API CONNECTION...")
try:
    from aerodatabox_client import AeroDataBoxClient
    client = AeroDataBoxClient()
    
    # Test airport info
    print("   Testing DEL airport info...")
    result = client.get_airport_info('DEL')
    if result:
        print(f"   ✅ Success: {result.get('name', 'Unknown')}")
    else:
        print("   ❌ Failed to get airport info")
    
    # Test flights
    print("   Testing DEL departures...")
    result = client.get_airport_flights('DEL', 'departures')
    if result and 'data' in result:
        print(f"   ✅ Success: {len(result['data'])} flights found")
    else:
        print("   ❌ Failed to get flights")
        
except Exception as e:
    print(f"❌ API test error: {e}")

# Check 4: Data Fetcher
print("\n4️⃣ TESTING DATA FETCHER...")
try:
    from data_fetcher import SmartDataFetcher
    fetcher = SmartDataFetcher(db)
    
    print("   Fetching dashboard data...")
    result = fetcher.fetch_dashboard_data()
    
    if result.get('success'):
        print(f"   ✅ Fetch successful:")
        print(f"      Airports: {result.get('airports_fetched', 0)}")
        print(f"      Flights: {result.get('flights_fetched', 0)}")
        print(f"      Time: {result.get('time', 0):.1f}s")
    else:
        print(f"   ❌ Fetch failed: {result.get('error', 'Unknown error')}")
        
except Exception as e:
    print(f"❌ Fetcher error: {e}")

print("\n" + "=" * 60)
print("📊 DIAGNOSIS COMPLETE")
print("\nIf you see ❌ errors above, those are the issues to fix.")
print("If all checks show ✅ but dashboard is empty, run:")
print("   python debug.py 2>&1 | grep -A5 'TESTING DATA FETCHER'")