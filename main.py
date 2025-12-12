import streamlit as st
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.home import home_page
from app.flights import flights_page
from app.airports import airports_page
from app.analytics import analytics_page
from app.utils import init_database

# Page configuration
st.set_page_config(
    page_title="Air Tracker: Flight Analytics",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 2rem;
    }
    .stButton button {
        width: 100%;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        margin: 10px 0;
    }
    </style>
""", unsafe_allow_html=True)

# Sidebar navigation
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/824/824100.png", width=100)
    st.title("✈️ Air Tracker")
    st.markdown("---")
    
    page = st.radio(
        "Navigation",
        ["🏠 Dashboard", "🛫 Flight Operations", "🏢 Airport Analytics", "📊 Advanced Analytics"]
    )
    
    st.markdown("---")
    
    # Database status
    st.subheader("Database Status")
    db = init_database()
    if db:
        st.success("✅ Connected to Database")
        
        # Quick stats
        try:
            from database.queries import FlightQueries
            import pandas as pd
            
            # Get counts
            airports_count = db.execute_query("SELECT COUNT(*) as count FROM airport", fetch=True)[0]['count']
            flights_count = db.execute_query("SELECT COUNT(*) as count FROM flights", fetch=True)[0]['count']
            aircraft_count = db.execute_query("SELECT COUNT(*) as count FROM aircraft", fetch=True)[0]['count']
            
            st.metric("Airports", airports_count)
            st.metric("Flights", flights_count)
            st.metric("Aircraft", aircraft_count)
        except:
            pass
    else:
        st.error("❌ Database Connection Failed")
    
    st.markdown("---")
    
    # Info section
    st.subheader("About")
    st.info("""
        **Air Tracker: Flight Analytics**
        
        A comprehensive flight data analytics platform
        using AeroDataBox API and MySQL database.
        
        **Features:**
        - Real-time flight tracking
        - Airport performance analytics
        - Delay analysis
        - Route optimization insights
    """)

# Main content area
if page == "🏠 Dashboard":
    home_page()
elif page == "🛫 Flight Operations":
    flights_page()
elif page == "🏢 Airport Analytics":
    airports_page()
elif page == "📊 Advanced Analytics":
    analytics_page()

# Footer
st.markdown("---")
col1, col2, col3 = st.columns(3)
with col2:
    st.caption("© 2024 Air Tracker Flight Analytics | Built with Streamlit & MySQL")

# Add refresh button in sidebar
with st.sidebar:
    st.markdown("---")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.rerun()