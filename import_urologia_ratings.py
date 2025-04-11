"""
Script per l'importazione delle valutazioni di Urologia da CSV in modalità non interattiva

Questo script importa SOLO i punteggi della specialità Urologia da un file CSV.
Il processo è completamente automatico e non richiede input dell'utente.
"""

import sys
import os
import csv
import logging
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import Session
from app import app, db
from models import MedicalFacility, Specialty, FacilitySpecialty
from backup_database import backup_database

# Setup logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_facility_id_by_name_and_city(session, name, city):
    """Get facility ID by name and city"""
    facility = session.query(MedicalFacility).filter(
        MedicalFacility.name.ilike(f"%{name}%"),
        MedicalFacility.city.ilike(f"%{city}%")
    ).first()
    
    if facility:
        return facility.id
    return None

def import_urologia_ratings(csv_file, batch_size=20):
    """
    Import ONLY Urologia ratings from a CSV file.
    
    Args:
        csv_file: Path to the CSV file
        batch_size: Number of records to process in each batch
        
    Returns:
        dict: Statistics about the import operation
    """
    stats = {
        'processed': 0,
        'updated': 0,
        'created': 0,
        'errors': 0,
        'facilities_not_found': 0
    }
    
    try:
        # First, create a backup of the database
        backup_file = backup_database()
        logger.info(f"Backup created: {backup_file}")
        
        # Use the specific ID for Urologia
        urologia_id = 791
        
        # Read the CSV file
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            all_rows = list(reader)
            logger.info(f"Found {len(all_rows)} rows in CSV file")
            
            # Process in batches
            for i in range(0, len(all_rows), batch_size):
                batch = all_rows[i:i+batch_size]
                logger.info(f"Processing batch {i//batch_size + 1} of {(len(all_rows) - 1) // batch_size + 1}")
                
                with Session(db.engine) as session:
                    # Verify Urologia exists with expected ID
                    specialty = session.query(Specialty).filter_by(id=urologia_id).first()
                    if not specialty or specialty.name != 'Urologia':
                        logger.error(f"Urologia specialty not found with ID {urologia_id}!")
                        return stats
                    
                    # Process facilities in this batch
                    for row in batch:
                        facility_name = row['Name of the facility']
                        city = row['City']
                        urologia_rating = row['Urologia'].strip()
                        
                        # Skip empty ratings
                        if not urologia_rating:
                            logger.debug(f"Empty Urologia rating for {facility_name}, skipping")
                            continue
                        
                        # Find the facility ID
                        facility_id = get_facility_id_by_name_and_city(session, facility_name, city)
                        
                        if not facility_id:
                            logger.warning(f"Facility not found: {facility_name} in {city}")
                            stats['facilities_not_found'] += 1
                            continue
                        
                        stats['processed'] += 1
                        
                        try:
                            # Convert rating to float
                            quality_rating = float(urologia_rating)
                            # Ensure rating is within valid range (1-5)
                            quality_rating = max(1.0, min(5.0, quality_rating))
                            
                            # Check if facility-specialty relationship exists
                            facility_specialty = session.query(FacilitySpecialty).filter_by(
                                facility_id=facility_id, 
                                specialty_id=urologia_id
                            ).first()
                            
                            if facility_specialty:
                                # Update existing relationship
                                old_rating = facility_specialty.quality_rating
                                
                                if old_rating is None or abs(old_rating - quality_rating) > 0.001:
                                    facility_specialty.quality_rating = quality_rating
                                    logger.info(f"Updated: {facility_name} ({facility_id}), Urologia rating: {old_rating} -> {quality_rating}")
                                    stats['updated'] += 1
                            else:
                                # Create new relationship
                                new_relationship = FacilitySpecialty(
                                    facility_id=facility_id,
                                    specialty_id=urologia_id,
                                    quality_rating=quality_rating
                                )
                                session.add(new_relationship)
                                logger.info(f"Created: {facility_name} ({facility_id}), Urologia rating: {quality_rating}")
                                stats['created'] += 1
                        
                        except Exception as e:
                            logger.error(f"Error processing {facility_name} Urologia rating: {e}")
                            stats['errors'] += 1
                    
                    # Commit changes for this batch
                    session.commit()
                    logger.info(f"Committed changes for batch {i//batch_size + 1}")
                    logger.info(f"Progress: Created {stats['created']}, Updated {stats['updated']}")
        
        # Update database status log
        update_database_status("Urologia ratings imported from CSV")
        
        return stats
    
    except Exception as e:
        logger.error(f"Error importing Urologia ratings: {e}")
        raise

def update_database_status(message):
    """Update the database status to reflect the update operation"""
    try:
        with Session(db.engine) as session:
            # Check if database_status table exists
            result = session.execute(text("SELECT to_regclass('database_status')")).scalar()
            
            if result:
                # Update the status
                timestamp = datetime.now()
                session.execute(
                    text("UPDATE database_status SET status = :status, last_updated = :timestamp"),
                    {"status": "updated", "timestamp": timestamp}
                )
                session.execute(
                    text("INSERT INTO database_status_log (status, message, timestamp) VALUES (:status, :message, :timestamp)"),
                    {"status": "updated", "message": message, "timestamp": timestamp}
                )
                session.commit()
                logger.info(f"Updated database status: {message}")
            else:
                logger.warning("database_status table not found, skipping status update")
    
    except Exception as e:
        logger.error(f"Error updating database status: {e}")

def print_stats(stats):
    """Print statistics about the import operation"""
    print("\nImport completed:")
    print(f"  Processed:             {stats['processed']} ratings")
    print(f"  Updated:               {stats['updated']} ratings")
    print(f"  Created:               {stats['created']} new ratings")
    print(f"  Errors:                {stats['errors']} ratings")
    print(f"  Facilities not found:  {stats['facilities_not_found']} facilities")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python import_urologia_ratings.py path/to/ratings.csv")
        sys.exit(1)
    
    # Use the Flask application context
    with app.app_context():
        csv_file = sys.argv[1]
        print(f"Importing Urologia ratings from {csv_file}...")
        
        stats = import_urologia_ratings(csv_file)
        print_stats(stats)
        
        print("\nUrologia ratings have been imported.")
        print("A backup of the previous database state has been created.")