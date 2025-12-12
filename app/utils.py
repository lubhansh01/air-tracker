import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from database.db_setup import DatabaseManager

def init_database():
    """Initialize database connection"""
    if 'db' not in st.session_state:
        db = DatabaseManager()
        if db.connect():
            st.session_state.db = db
        else:
            st.error("Failed to connect to database")
            return None
    return st.session_state.db

def get_dataframe_from_query(query, params=None):
    """Execute query and return DataFrame"""
    db = init_database()
    if db:
        result = db.execute_query(query, params, fetch=True)
        if result:
            return pd.DataFrame(result)
    return pd.DataFrame()

def plot_bar_chart(df, x, y, title, color=None):
    """Create bar chart"""
    fig = px.bar(df, x=x, y=y, title=title, color=color,
                 text=y, template='plotly_white')
    fig.update_traces(texttemplate='%{text:.0f}', textposition='outside')
    fig.update_layout(
        xaxis_tickangle=-45,
        height=400,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    return fig

def plot_pie_chart(df, names, values, title):
    """Create pie chart"""
    fig = px.pie(df, names=names, values=values, title=title,
                 hole=0.3, template='plotly_white')
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig

def plot_line_chart(df, x, y, title, color=None):
    """Create line chart"""
    fig = px.line(df, x=x, y=y, title=title, color=color,
                  markers=True, template='plotly_white')
    fig.update_layout(
        height=400,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    return fig

def plot_geographic_map(airports_df):
    """Create geographic map of airports"""
    fig = px.scatter_geo(airports_df,
                         lat='latitude',
                         lon='longitude',
                         hover_name='name',
                         hover_data=['city', 'country'],
                         size_max=15,
                         template='plotly_white',
                         title='Airports Location Map')
    
    fig.update_geos(
        projection_type="natural earth",
        showcountries=True,
        countrycolor="Black",
        showsubunits=True,
        subunitcolor="Blue"
    )
    
    fig.update_layout(height=500)
    return fig

def format_flight_status(status):
    """Format flight status with colors"""
    status_colors = {
        'Scheduled': '🟡',
        'On Time': '🟢',
        'Delayed': '🟠',
        'Cancelled': '🔴',
        'Completed': '🔵'
    }
    return f"{status_colors.get(status, '⚪')} {status}"

def calculate_flight_duration(departure, arrival):
    """Calculate flight duration in hours"""
    if departure and arrival:
        try:
            dep_time = pd.to_datetime(departure)
            arr_time = pd.to_datetime(arrival)
            duration = arr_time - dep_time
            return round(duration.total_seconds() / 3600, 1)
        except:
            return None
    return None

def get_date_range_options():
    """Get date range options for filters"""
    today = datetime.now()
    return {
        "Last 7 days": (today - timedelta(days=7), today),
        "Last 30 days": (today - timedelta(days=30), today),
        "Last 90 days": (today - timedelta(days=90), today),
        "Custom Range": None
    }

def display_metrics(metrics_dict):
    """Display metrics in columns"""
    cols = st.columns(len(metrics_dict))
    for idx, (title, value) in enumerate(metrics_dict.items()):
        with cols[idx]:
            st.metric(label=title, value=value)