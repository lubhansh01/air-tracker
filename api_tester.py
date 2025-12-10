"""
Test script to verify API connectivity
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from aerodatabox_client import AeroDataBoxClient

def test_connection():
    """Test basic API connection"""
    print("🔍 Testing AeroDataBox API Connection...")
    print("=" * 50)
    
    client = AeroDataBoxClient()
    
    # Test 1: Airport Info (should work)
    print("\n1. Testing airport info (DEL)...")
    result = client.get_airport_info('DEL')
    if result:
        print(f"   ✅ Success: {result.get('name')}")
    else:
        print("   ❌ Failed")
    
    # Test 2: Flight Schedule (should work)
    print("\n2. Testing flight schedule (DEL departures)...")
    result = client.get_airport_flights('DEL', 'departures')
    if result and 'data' in result:
        print(f"   ✅ Success: {len(result['data'])} flights")
    else:
        print("   ❌ Failed")
    
    # Test 3: Delays (may work)
    print("\n3. Testing delay statistics (DEL)...")
    result = client.get_airport_delays('DEL')
    if result:
        print(f"   ✅ Success: Delay data received")
    else:
        print("   ⚠️ Note: Delay endpoint may not be available")
    
    print("\n" + "=" * 50)
    print("✅ Test completed")
    
    # Show stats
    stats = client.get_stats()
    print(f"\n📊 API Stats:")
    print(f"   Successful requests: {stats['successful']}")
    print(f"   Failed requests: {stats['failed']}")
    print(f"   Cache hits: {stats['cache_hits']}")

if __name__ == "__main__":
    test_connection()