"""
Quick test script to verify all endpoints are working
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from aerodatabox_client import AeroDataBoxClient
from config import AIRPORT_CODES

def test_all_endpoints():
    """Test all major endpoints"""
    client = AeroDataBoxClient()
    
    print("🧪 Testing AeroDataBox API Endpoints...")
    print("=" * 50)
    
    # Test 1: Airport Info
    print("\n1. Testing 🌎 Get Airport endpoint...")
    for code in AIRPORT_CODES[:2]:  # Test first 2
        result = client.get_airport_info(code)
        if result:
            print(f"   ✓ {code}: {result.get('name', 'No name')}")
        else:
            print(f"   ✗ {code}: Failed")
    
    # Test 2: Airport Schedule
    print("\n2. Testing 🗓️ Airport Schedule endpoint...")
    result = client.get_airport_schedule('DEL', 'departures')
    if result and 'data' in result:
        print(f"   ✓ DEL departures: {len(result['data'])} flights")
    else:
        print("   ✗ Failed to get schedule")
    
    # Test 3: Aircraft Info
    print("\n3. Testing ✈️ Aircraft Info endpoint...")
    # Try a common aircraft registration pattern
    result = client.get_aircraft_info('VT-ALV')  # Sample Air India aircraft
    if result:
        print(f"   ✓ Aircraft: {result.get('model', {}).get('text', 'Unknown')}")
    else:
        print("   ✗ Failed to get aircraft info")
    
    # Test 4: Airport Delays
    print("\n4. Testing 📊 Airport Delays endpoint...")
    result = client.get_airport_delays('DEL')
    if result:
        stats = result.get('statistics', {})
        print(f"   ✓ DEL delays: {stats.get('flights', {}).get('delayed', 0)} delayed flights")
    else:
        print("   ✗ Failed to get delays")
    
    # Test 5: Multiple airports in parallel
    print("\n5. Testing parallel airport fetching...")
    results = client.get_multiple_airports(['DEL', 'BOM', 'LHR'])
    print(f"   ✓ Fetched {len([r for r in results.values() if r])} airports in parallel")
    
    print("\n" + "=" * 50)
    print("✅ All tests completed!")
    
    # Show cache stats
    print(f"\n📦 Cache entries: {len(client.cache)}")

if __name__ == "__main__":
    test_all_endpoints()