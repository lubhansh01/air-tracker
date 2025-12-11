"""
Test and run the application
"""

import os
import sys
from dotenv import load_dotenv

def check_environment():
    """Check if everything is set up"""
    print("🔍 Checking environment...")
    
    load_dotenv()
    api_key = os.getenv('AERODATABOX_API_KEY')
    
    if not api_key or api_key == 'your_actual_api_key_here':
        print("❌ ERROR: Please update .env file with your API key")
        print("\nSteps:")
        print("1. Get API key from RapidAPI (AeroDataBox)")
        print("2. Edit .env file")
        print("3. Replace 'your_actual_api_key_here' with your key")
        return False
    
    print(f"✅ API Key found: {api_key[:10]}...")
    
    # Check if all files exist
    required_files = ['database.py', 'data_fetcher.py', 'dashboard.py', '.env']
    missing = []
    
    for file in required_files:
        if not os.path.exists(file):
            missing.append(file)
    
    if missing:
        print(f"❌ Missing files: {missing}")
        return False
    
    print("✅ All files present")
    return True

def test_imports():
    """Test if imports work"""
    print("\n🔍 Testing imports...")
    
    try:
        from database import FlightDatabase
        print("✅ database.py imports OK")
        
        from data_fetcher import SmartDataFetcher
        print("✅ data_fetcher.py imports OK")
        
        return True
    except Exception as e:
        print(f"❌ Import error: {e}")
        return False

if __name__ == "__main__":
    print("✈️ AIR TRACKER - 15 AIRPORTS")
    print("=" * 50)
    
    if check_environment() and test_imports():
        print("\n✅ Everything looks good!")
        print("\nTo run the dashboard:")
        print("   streamlit run dashboard.py")
        
        # Optional: Create database
        from database import FlightDatabase
        db = FlightDatabase()
        print("✅ Database initialized")
    else:
        print("\n❌ Please fix the issues above")