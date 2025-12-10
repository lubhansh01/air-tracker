"""
Main entry point for Air Tracker application
"""

import sys
import os
from dotenv import load_dotenv

def check_environment():
    """Check if all required environment variables are set"""
    load_dotenv()
    
    api_key = os.getenv('AERODATABOX_API_KEY')
    
    if not api_key or api_key == 'your_actual_api_key_here':
        print("❌ ERROR: Please update your API key in .env file")
        print("1. Get your API key from RapidAPI")
        print("2. Update .env file with: AERODATABOX_API_KEY=your_key_here")
        return False
    
    print("✅ Environment variables loaded successfully")
    return True

if __name__ == "__main__":
    print("🔍 Checking environment...")
    
    if check_environment():
        print("\n🚀 Air Tracker: Flight Analytics Dashboard")
        print("=" * 50)
        print("\nTo run the dashboard:")
        print("1. streamlit run dashboard.py")
        print("\nOr run tests:")
        print("2. python -c \"from aerodatabox_client import AeroDataBoxClient; client = AeroDataBoxClient(); print('Client initialized successfully')\"")
    else:
        sys.exit(1)