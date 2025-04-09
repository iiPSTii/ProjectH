"""
Migration Script to Add Geocoding Columns

This script adds the geocoding columns (latitude, longitude, geocoded) to the MedicalFacility table.
"""
import logging
from app import app, db
from models import MedicalFacility

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_geocoding_columns():
    """Add the geocoding columns to the MedicalFacility table"""
    with app.app_context():
        logger.info("Starting database migration to add geocoding columns...")
        
        try:
            # Direct execution of ALTER TABLE statements
            logger.info("Adding latitude column...")
            db.session.execute(db.text(
                "ALTER TABLE medical_facilities ADD COLUMN IF NOT EXISTS latitude FLOAT"
            ))
            
            logger.info("Adding longitude column...")
            db.session.execute(db.text(
                "ALTER TABLE medical_facilities ADD COLUMN IF NOT EXISTS longitude FLOAT"
            ))
            
            logger.info("Adding geocoded column...")
            db.session.execute(db.text(
                "ALTER TABLE medical_facilities ADD COLUMN IF NOT EXISTS geocoded BOOLEAN DEFAULT FALSE"
            ))
            
            # Commit the changes
            db.session.commit()
            
            logger.info("Migration completed successfully.")
        except Exception as e:
            logger.error(f"Error migrating database: {str(e)}")
            db.session.rollback()
            raise

if __name__ == "__main__":
    add_geocoding_columns()