"""
Main entry point
"""

import sys
import os
from dotenv import load_dotenv

def main():
    """Main function"""
    print("✈️ Air Tracker: Flight Analytics")
    print("=" * 40)
    
    # Check environment
    load_dotenv()
    api_key = os.getenv('AERODATABOX_API_KEY')
    
    if not api_key or api_key == 'your_actual_api_key_here':
        print("❌ ERROR: Please update your API key in .env file")
        print("\nSteps:")
        print("1. Get API key from RapidAPI (AeroDataBox)")
        print("2. Edit .env file and replace 'your_actual_api_key_here'")
        return False
    
    print("✅ Environment configured")
    print("\nTo run the dashboard:")
    print("  streamlit run dashboard.py")
    
    return True

if __name__ == "__main__":
    main()