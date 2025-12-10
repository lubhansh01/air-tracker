"""
Test the complete system
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_complete_system():
    """Test the complete system"""
    print("🧪 COMPREHENSIVE SYSTEM TEST")
    print("=" * 60)
    
    # Import all modules
    try:
        from database import FlightDatabase
        from aerodatabox_client import AeroDataBoxClient
        from data_fetcher import SmartDataFetcher
        print("✅ All modules imported successfully")
    except Exception as e:
        print(f"❌ Import error: {e}")
        return
    
    # Test 1: Database
    print("\n1. DATABASE TEST")
    try:
        db = FlightDatabase()
        print("   ✅ Database connected")
        
        # Create sample data if empty
        airports = db.execute_query("SELECT COUNT(*) FROM airport")[0][0]
        flights = db.execute_query("SELECT COUNT(*) FROM flights")[0][0]
        print(f"   📊 Current data: {airports} airports, {flights} flights")
        
    except Exception as e:
        print(f"   ❌ Database error: {e}")
    
    # Test 2: API Client
    print("\n2. API CLIENT TEST")
    try:
        client = AeroDataBoxClient()
        print("   ✅ API client initialized")
        
        # Quick test
        result = client.get_airport_info('DEL')
        if result:
            print(f"   ✅ API working: {result.get('name', 'Test passed')}")
        else:
            print("   ⚠️ API returned no data (might be rate limited)")
            
    except Exception as e:
        print(f"   ❌ API client error: {e}")
    
    # Test 3: Data Fetcher
    print("\n3. DATA FETCHER TEST")
    try:
        fetcher = SmartDataFetcher(db)
        print("   ✅ Fetcher initialized")
        
        # Run a mini-fetch
        print("   Running quick fetch...")
        result = fetcher.fetch_dashboard_data()
        
        if result.get('success'):
            print(f"   ✅ Fetch successful!")
            print(f"      Airports: {result.get('airports_fetched', 0)}")
            print(f"      Flights: {result.get('flights_fetched', 0)}")
            print(f"      Time: {result.get('time', 0):.1f}s")
        else:
            print(f"   ❌ Fetch failed: {result.get('error', 'Unknown')}")
            
    except Exception as e:
        print(f"   ❌ Fetcher error: {e}")
        import traceback
        traceback.print_exc()
    
    # Final check
    print("\n" + "=" * 60)
    print("🎯 FINAL RECOMMENDATIONS:")
    print()
    
    # Check data in database
    final_airports = db.execute_query("SELECT COUNT(*) FROM airport")[0][0]
    final_flights = db.execute_query("SELECT COUNT(*) FROM flights")[0][0]
    
    if final_flights > 0:
        print(f"✅ SUCCESS! Database has {final_flights} flights")
        print("   Run: streamlit run dashboard.py")
    else:
        print("�️ ISSUE: No flights in database")
        print("   Possible causes:")
        print("   1. API key not set in .env file")
        print("   2. API rate limiting")
        print("   3. Network issues")
        print()
        print("   Check .env file contains:")
        print("   AERODATABOX_API_KEY=your_actual_key_here")
        print("   AERODATABOX_API_HOST=aerodatabox.p.rapidapi.com")

if __name__ == "__main__":
    test_complete_system()