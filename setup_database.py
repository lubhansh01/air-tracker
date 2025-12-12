#!/usr/bin/env python3
"""
Database Setup Script
Run this script to set up the database and create tables
"""

import os
import sys
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_setup import DatabaseManager

def setup_database():
    """Set up the database and create tables"""
    print("🚀 Setting up Flight Analytics Database...")
    
    # Load environment variables
    load_dotenv()
    
    # Initialize database manager
    db_manager = DatabaseManager()
    
    # Connect to database
    if db_manager.connect():
        print("✅ Connected to MySQL server")
        
        # Create tables
        if db_manager.create_tables():
            print("✅ Database tables created successfully")
            
            # Verify tables were created
            cursor = db_manager.connection.cursor()
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            print("\n📋 Database Tables Created:")
            for table in tables:
                print(f"  - {table[0]}")
            
            cursor.close()
        else:
            print("❌ Failed to create tables")
    else:
        print("❌ Failed to connect to database")
    
    db_manager.close()
    print("\n🎉 Database setup completed!")

if __name__ == "__main__":
    setup_database()