"""
Main entry point with health check
"""
import streamlit as st
from dashboard import st  # Import streamlit from dashboard
import sys
import os

def health_check():
    """Check if all components are working"""
    print("🔍 Running health check...")
    
    # Check environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    api_key = os.getenv('AERODATABOX_API_KEY')
    if not api_key or api_key == 'your_actual_api_key_here':
        print("❌ Please update your API key in .env file")
        return False
    
    print("✅ Environment variables loaded")
    return True

if __name__ == "__main__":
    if health_check():
        print("🚀 Starting Air Tracker Dashboard...")
        # The dashboard will be run by streamlit
    else:
        print("❌ Health check failed. Please fix issues above.")