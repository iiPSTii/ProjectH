"""
Script per l'importazione di ratings da CSV con formato multi-colonna

Questo script importa i punteggi di specialità da un file CSV
con formato multi-colonna (nome struttura, città, e punteggi per diverse specialità).
Prima di ogni aggiornamento, viene creato un backup completo del database.

Formato CSV atteso:
Name of the facility,City,Cardiologia,Ortopedia,Oncologia,Neurologia,Urologia,Chirurgia generale,Pediatria,Ginecologia

Uso:
  python import_new_format_ratings.py path/to/ratings.csv
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

def get_specialty_id_by_name(session, name):
    """Get specialty ID by name"""
    specialty = session.query(Specialty).filter(Specialty.name.ilike(f"%{name}%")).first()
    if specialty:
        return specialty.id
    return None

def get_facility_id_by_name_and_city(session, name, city):
    """Get facility ID by name and city"""
    facility = session.query(MedicalFacility).filter(
        MedicalFacility.name.ilike(f"%{name}%"),
        MedicalFacility.city.ilike(f"%{city}%")
    ).first()
    
    if facility:
        return facility.id
    return None

def import_ratings_new_format(csv_file):
    """
    Import ratings from a CSV file with the new multi-column format.
    
    CSV format:
    Name of the facility,City,Cardiologia,Ortopedia,Oncologia,Neurologia,Urologia,Chirurgia generale,Pediatria,Ginecologia
    
    Returns:
        dict: Statistics about the import operation
    """
    stats = {
        'processed': 0,
        'updated': 0,
        'created': 0,
        'errors': 0,
        'facilities_not_found': 0,
        'specialties_not_found': 0
    }
    
    try:
        # First, back up the database
        logger.info("Creating database backup...")
        backup_file = backup_database()
        logger.info(f"Backup created: {backup_file}")
        
        # Now process the CSV file
        logger.info(f"Processing ratings from {csv_file}...")
        
        # Read the CSV file
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            # Get all specialty names from the CSV headers
            specialty_fields = [field for field in reader.fieldnames 
                               if field not in ['Name of the facility', 'City']]
            
            logger.info(f"Processing {len(rows)} facilities with {len(specialty_fields)} specialties")
            
            # Process each row
            with Session(db.engine) as session:
                # Get specialty IDs map
                specialty_ids = {}
                for specialty_name in specialty_fields:
                    specialty_id = get_specialty_id_by_name(session, specialty_name)
                    if specialty_id:
                        specialty_ids[specialty_name] = specialty_id
                    else:
                        logger.warning(f"Specialty not found: {specialty_name}")
                        stats['specialties_not_found'] += 1
                
                # Process each facility in batches
                batch_size = 10
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i+batch_size]
                    process_facility_batch(session, batch, specialty_fields, specialty_ids, stats)
                    session.commit()
                    logger.info(f"Processed batch {i//batch_size + 1}, {stats['processed']} ratings so far")
        
        # Update the database status to reflect the changes
        update_database_status(f"Specialty ratings updated from {csv_file}")
        
        return stats
    
    except Exception as e:
        logger.error(f"Error importing ratings: {e}")
        # Rollback any uncommitted changes
        try:
            session.rollback()
        except:
            pass
        raise

def process_facility_batch(session, batch, specialty_fields, specialty_ids, stats):
    """Process a batch of facilities"""
    for row in batch:
        facility_name = row['Name of the facility']
        city = row['City']
        
        # Find the facility ID
        facility_id = get_facility_id_by_name_and_city(session, facility_name, city)
        
        if not facility_id:
            logger.warning(f"Facility not found: {facility_name} in {city}")
            stats['facilities_not_found'] += 1
            continue
        
        # Process each specialty rating
        for specialty_name in specialty_fields:
            stats['processed'] += 1
            
            try:
                if specialty_name not in specialty_ids:
                    stats['errors'] += 1
                    continue
                
                specialty_id = specialty_ids[specialty_name]
                rating_str = row[specialty_name].strip()
                
                # Skip empty ratings
                if not rating_str:
                    continue
                
                # Convert rating to float
                try:
                    quality_rating = float(rating_str)
                    # Ensure rating is within valid range (1-5)
                    quality_rating = max(1.0, min(5.0, quality_rating))
                except (ValueError, TypeError):
                    logger.warning(f"Invalid rating for {facility_name}, {specialty_name}: {rating_str}")
                    stats['errors'] += 1
                    continue
                
                # Check if facility-specialty relationship exists
                facility_specialty = session.query(FacilitySpecialty).filter_by(
                    facility_id=facility_id, 
                    specialty_id=specialty_id
                ).first()
                
                if facility_specialty:
                    # Update existing relationship
                    old_rating = facility_specialty.quality_rating
                    
                    # Se il vecchio rating è None o diverso dal nuovo, aggiorniamo
                    if old_rating is None or abs(old_rating - quality_rating) > 0.001:
                        facility_specialty.quality_rating = quality_rating
                        logger.debug(f"Updated: {facility_name} ({facility_id}), {specialty_name} ({specialty_id}): {old_rating} -> {quality_rating}")
                        stats['updated'] += 1
                else:
                    # Create new relationship
                    new_relationship = FacilitySpecialty(
                        facility_id=facility_id,
                        specialty_id=specialty_id,
                        quality_rating=quality_rating
                    )
                    session.add(new_relationship)
                    logger.debug(f"Created: {facility_name} ({facility_id}), {specialty_name} ({specialty_id}): {quality_rating}")
                    stats['created'] += 1
            
            except Exception as e:
                logger.error(f"Error processing {facility_name}, {specialty_name}: {e}")
                stats['errors'] += 1

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

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python import_new_format_ratings.py path/to/ratings.csv")
        sys.exit(1)
    
    # Use the Flask application context
    with app.app_context():
        csv_file = sys.argv[1]
        print(f"Importing ratings from {csv_file}...")
        stats = import_ratings_new_format(csv_file)
        
        print("\nImport completed:")
        print(f"  Processed:             {stats['processed']} ratings")
        print(f"  Updated:               {stats['updated']} ratings")
        print(f"  Created:               {stats['created']} new ratings")
        print(f"  Errors:                {stats['errors']} ratings")
        print(f"  Facilities not found:  {stats['facilities_not_found']} facilities")
        print(f"  Specialties not found: {stats['specialties_not_found']} specialties")
        
        print("\nDatabase has been updated with new specialty ratings.")
        print("A backup of the previous database state has been created.")