"""
Add specialty columns to medical_facilities table

This script adds the necessary specialty rating columns and strengths summary column to the
medical_facilities table in the PostgreSQL database.
"""

import os
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add the current directory to the Python path
sys.path.append('.')

# Import app and db
from app import app, db

def add_specialty_columns():
    """
    Add specialty rating columns and strengths summary column to the medical_facilities table
    """
    try:
        with app.app_context():
            # Get database connection
            conn = db.engine.connect()
            
            # Add specialty rating columns
            specialties = ['cardiology', 'orthopedics', 'oncology', 'neurology', 
                          'surgery', 'urology', 'pediatrics', 'gynecology']
            
            for specialty in specialties:
                column_name = f"{specialty}_rating"
                alter_query = f"ALTER TABLE medical_facilities ADD COLUMN IF NOT EXISTS {column_name} FLOAT"
                conn.execute(alter_query)
                logger.info(f"Added column: {column_name}")
            
            # Add strengths summary column
            alter_query = "ALTER TABLE medical_facilities ADD COLUMN IF NOT EXISTS strengths_summary TEXT"
            conn.execute(alter_query)
            logger.info("Added column: strengths_summary")
            
            # Commit changes
            conn.commit()
            
            logger.info("All specialty columns added successfully")
            return True
    except Exception as e:
        logger.error(f"Error adding specialty columns: {str(e)}")
        return False

if __name__ == "__main__":
    if add_specialty_columns():
        print("✅ Specialty columns added successfully to medical_facilities table")
    else:
        print("❌ Failed to add specialty columns")