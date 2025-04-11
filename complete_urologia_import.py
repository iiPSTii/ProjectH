"""
Script per completare l'importazione delle valutazioni di Urologia

Questo script importa SOLO i punteggi della specialità Urologia 
per le strutture che non hanno ancora una valutazione per questa specialità.
Utilizza batch più piccoli per evitare timeout.
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

def get_facilities_with_urologia():
    """Get IDs of facilities that already have Urologia specialty"""
    with Session(db.engine) as session:
        results = session.execute(text(
            "SELECT facility_id FROM facility_specialty WHERE specialty_id = 791"
        )).fetchall()
        return [r[0] for r in results]

def complete_urologia_import(csv_file, batch_size=10):
    """
    Import remaining Urologia ratings from a CSV file.
    
    Args:
        csv_file: Path to the CSV file
        batch_size: Number of records to process in each batch
        
    Returns:
        dict: Statistics about the import operation
    """
    stats = {
        'processed': 0,
        'created': 0,
        'errors': 0,
        'facilities_not_found': 0,
        'already_imported': 0
    }
    
    try:
        # Get facilities that already have Urologia ratings
        existing_facility_ids = set(get_facilities_with_urologia())
        logger.info(f"Found {len(existing_facility_ids)} facilities with existing Urologia ratings")
        
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
                batch_num = i//batch_size + 1
                total_batches = (len(all_rows) - 1) // batch_size + 1
                logger.info(f"Processing batch {batch_num} of {total_batches}")
                
                with Session(db.engine) as session:
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
                        
                        # Skip if already imported
                        if facility_id in existing_facility_ids:
                            stats['already_imported'] += 1
                            logger.debug(f"Facility {facility_name} already has Urologia rating, skipping")
                            continue
                        
                        stats['processed'] += 1
                        
                        try:
                            # Convert rating to float
                            quality_rating = float(urologia_rating)
                            # Ensure rating is within valid range (1-5)
                            quality_rating = max(1.0, min(5.0, quality_rating))
                            
                            # Create new relationship
                            new_relationship = FacilitySpecialty(
                                facility_id=facility_id,
                                specialty_id=urologia_id,
                                quality_rating=quality_rating
                            )
                            session.add(new_relationship)
                            logger.info(f"Created: {facility_name} ({facility_id}), Urologia rating: {quality_rating}")
                            stats['created'] += 1
                            
                            # Add to existing set to avoid duplicates
                            existing_facility_ids.add(facility_id)
                        
                        except Exception as e:
                            logger.error(f"Error processing {facility_name} Urologia rating: {e}")
                            stats['errors'] += 1
                    
                    # Commit changes for this batch
                    session.commit()
                    logger.info(f"Committed changes for batch {batch_num}")
                    logger.info(f"Progress: Created {stats['created']}, Already imported {stats['already_imported']}")
                    
                    # Update status after each batch to avoid losing progress
                    if stats['created'] > 0:
                        update_database_status(f"Batch {batch_num}/{total_batches}: Added {stats['created']} Urologia ratings")
        
        # Final database status update
        if stats['created'] > 0:
            update_database_status(f"Completed Urologia ratings import. Added {stats['created']} new ratings.")
        
        return stats
    
    except Exception as e:
        logger.error(f"Error completing Urologia ratings import: {e}")
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
    print(f"  Created:               {stats['created']} new ratings")
    print(f"  Already imported:      {stats['already_imported']} ratings")
    print(f"  Errors:                {stats['errors']} ratings")
    print(f"  Facilities not found:  {stats['facilities_not_found']} facilities")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python complete_urologia_import.py path/to/ratings.csv")
        sys.exit(1)
    
    # Use the Flask application context
    with app.app_context():
        csv_file = sys.argv[1]
        print(f"Completing Urologia ratings import from {csv_file}...")
        
        stats = complete_urologia_import(csv_file)
        print_stats(stats)
        
        print("\nRemaining Urologia ratings have been imported.")