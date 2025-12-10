"""
Test script for 15 airports
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from config import AIRPORT_CODES, AIRPORT_NAMES

load_dotenv()

print("🧪 TESTING 15 AIRPORTS SYSTEM")
print("=" * 60)

print(f"\n📌 AIRPORTS BEING TRACKED ({len(AIRPORT_CODES)} total):")
print("-" * 40)

# Indian airports
print("\n🇮🇳 INDIAN AIRPORTS (7):")
indian = [code for code in AIRPORT_CODES if code in ['DEL', 'BOM', 'MAA', 'BLR', 'HYD', 'CCU', 'AMD']]
for code in indian:
    print(f"   ✈️ {code} - {AIRPORT_NAMES.get(code, code)}")

# International airports
print("\n🌍 INTERNATIONAL AIRPORTS (8):")
international = [code for code in AIRPORT_CODES if code not in indian]
for code in international:
    print(f"   ✈️ {code} - {AIRPORT_NAMES.get(code, code)}")

print("\n" + "=" * 60)
print("To run the dashboard:")
print("   streamlit run dashboard.py")
print("\nTo fetch all data:")
print("   1. Open dashboard")
print("   2. Click 'Fetch All Data' in sidebar")
print("   3. View analytics for 15 airports")