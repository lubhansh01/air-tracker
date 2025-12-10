"""
Test cURL commands from AeroDataBox
"""

import os
import subprocess
import json
from dotenv import load_dotenv

load_dotenv()

def test_curl_command():
    """Test a single cURL command"""
    api_key = os.getenv('AERODATABOX_API_KEY')
    api_host = 'aerodatabox.p.rapidapi.com'
    
    # Example from AeroDataBox platform
    curl_cmd = [
        'curl',
        '--request', 'GET',
        '--url', f'https://{api_host}/airports/iata/DEL',
        '--header', f'x-rapidapi-key: {api_key}',
        '--header', f'x-rapidapi-host: {api_host}',
        '--silent'
    ]
    
    print(f"🔍 Testing: {' '.join(curl_cmd[3:7])}")
    
    try:
        result = subprocess.run(
            curl_cmd,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            print(f"✅ Success! Airport: {data.get('name')}")
            print(f"   City: {data.get('municipalityName')}")
            print(f"   Country: {data.get('country', {}).get('name')}")
            return True
        else:
            print(f"❌ Failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_all_endpoints():
    """Test all endpoints"""
    api_key = os.getenv('AERODATABOX_API_KEY')
    api_host = 'aerodatabox.p.rapidapi.com'
    
    endpoints = [
        ("Airport Info", "/airports/iata/DEL"),
        ("Flights Departures", "/flights/airports/iata/DEL/departures"),
        ("Flight Status", "/flights/number/AI101/2024-12-10"),
        ("Aircraft Info", "/aircrafts/reg/VT-ALV"),
        ("Airport Delays", "/airports/iata/DEL/delays"),
        ("Global Delays", "/airports/delays"),
        ("Countries", "/countries")
    ]
    
    print("🧪 TESTING ALL ENDPOINTS")
    print("=" * 50)
    
    for name, endpoint in endpoints:
        print(f"\n📡 {name}: {endpoint}")
        
        curl_cmd = [
            'curl',
            '--request', 'GET',
            '--url', f'https://{api_host}{endpoint}',
            '--header', f'x-rapidapi-key: {api_key}',
            '--header', f'x-rapidapi-host: {api_host}',
            '--silent',
            '--max-time', '10'
        ]
        
        try:
            result = subprocess.run(
                curl_cmd,
                capture_output=True,
                text=True,
                timeout=15
            )
            
            if result.returncode == 0:
                try:
                    data = json.loads(result.stdout)
                    print(f"   ✅ Success")
                    
                    # Show some info
                    if name == "Airport Info":
                        print(f"      Name: {data.get('name', 'N/A')}")
                    elif name == "Flights Departures":
                        flights = data.get('data', [])
                        print(f"      Flights: {len(flights)}")
                    elif name == "Flight Status":
                        print(f"      Status: {data.get('status', 'N/A')}")
                    elif name == "Aircraft Info":
                        print(f"      Model: {data.get('model', {}).get('text', 'N/A')}")
                    elif "Delays" in name:
                        print(f"      Data received")
                    elif name == "Countries":
                        countries = data.get('countries', [])
                        print(f"      Countries: {len(countries)}")
                        
                except json.JSONDecodeError:
                    print(f"   ⚠️ Invalid JSON: {result.stdout[:100]}")
            else:
                print(f"   ❌ Failed: {result.stderr[:100]}")
                
        except subprocess.TimeoutExpired:
            print(f"   ⏱️ Timeout")
        except Exception as e:
            print(f"   ❌ Error: {e}")
        
        time.sleep(1)  # Rate limiting
    
    print("\n" + "=" * 50)
    print("✅ Testing complete")

if __name__ == "__main__":
    if not os.getenv('AERODATABOX_API_KEY'):
        print("❌ Please set AERODATABOX_API_KEY in .env file")
    else:
        test_all_endpoints()