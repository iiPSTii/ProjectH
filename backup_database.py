"""
Backup Database Script

This script creates a full backup of the PostgreSQL database before performing
any updates to the specialty ratings. It exports all tables to CSV files
and compresses them into a timestamped ZIP archive.
"""

import os
import csv
import zipfile
import datetime
from contextlib import closing
import logging
from sqlalchemy.orm import Session
from app import app, db
from models import MedicalFacility, Region, Specialty, FacilitySpecialty

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def backup_database():
    """Export the entire database to a timestamped ZIP archive of CSV files"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = f"database_backup_{timestamp}"
    
    # Create a directory for the backup
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    
    try:
        # Back up each table to a CSV file
        tables = {
            'regions': Region,
            'specialties': Specialty, 
            'medical_facilities': MedicalFacility,
            'facility_specialties': FacilitySpecialty
        }
        
        # Export each table
        with Session(db.engine) as session:
            for table_name, model in tables.items():
                export_table_to_csv(session, model, f"{backup_dir}/{table_name}.csv")
        
        # Create a ZIP archive of all the CSV files
        zip_filename = f"database_backup_{timestamp}.zip"
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(backup_dir):
                for file in files:
                    zipf.write(os.path.join(root, file), 
                              os.path.relpath(os.path.join(root, file), 
                                             os.path.join(backup_dir, '..')))
        
        logger.info(f"Database backup completed: {zip_filename}")
        return zip_filename
    
    except Exception as e:
        logger.error(f"Error backing up database: {e}")
        raise
    finally:
        # Clean up the temporary directory
        import shutil
        if os.path.exists(backup_dir):
            shutil.rmtree(backup_dir)

def export_table_to_csv(session, model, output_file):
    """Export a table to a CSV file"""
    logger.info(f"Exporting {model.__tablename__} to {output_file}")
    
    try:
        # Get all records from the table
        records = session.query(model).all()
        
        if not records:
            logger.warning(f"No records found in {model.__tablename__}")
            return
        
        # Get the column names from the first record
        if hasattr(records[0], '__table__'):
            columns = [c.name for c in records[0].__table__.columns]
        else:
            # Try to get attributes from the first record
            columns = [k for k in records[0].__dict__.keys() if not k.startswith('_')]
        
        # Write the records to a CSV file
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            
            for record in records:
                row = []
                for column in columns:
                    if hasattr(record, column):
                        row.append(getattr(record, column))
                    else:
                        row.append(None)
                writer.writerow(row)
        
        logger.info(f"Exported {len(records)} records to {output_file}")
    
    except Exception as e:
        logger.error(f"Error exporting {model.__tablename__} to CSV: {e}")
        raise

if __name__ == "__main__":
    with app.app_context():
        backup_file = backup_database()
        print(f"Backup completed: {backup_file}")