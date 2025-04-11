"""
Script per l'importazione delle valutazioni di Urologia da CSV

Questo script importa SOLO i punteggi della specialità Urologia da un file CSV.
Prima esegue un test su un numero limitato di strutture (5) per verificare
che il processo funzioni correttamente.

Formato CSV atteso:
Name of the facility,City,Cardiologia,Ortopedia,Oncologia,Neurologia,Urologia,Chirurgia generale,Pediatria,Ginecologia
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

def import_urologia_ratings_test(csv_file, test_mode=True, max_facilities=5):
    """
    Import ONLY Urologia ratings from a CSV file.
    
    Args:
        csv_file: Path to the CSV file
        test_mode: If True, only process a few facilities for testing
        max_facilities: Maximum number of facilities to process in test mode
        
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
        # Get Urologia specialty ID - deve essere 791
        with Session(db.engine) as session:
            # Invece di cercare per nome, usiamo l'ID diretto per sicurezza
            urologia_id = 791
            
            # Verifichiamo che l'ID esista
            specialty = session.query(Specialty).filter_by(id=urologia_id).first()
            if not specialty or specialty.name != 'Urologia':
                logger.error(f"Urologia specialty not found with ID {urologia_id}!")
                urologia_id = get_specialty_id_by_name(session, "Urologia")
                if not urologia_id:
                    logger.error("Urologia specialty not found in the database!")
                    return stats
            
            logger.info(f"Found Urologia specialty with ID: {urologia_id}")
            
            # Read the CSV file
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                if test_mode:
                    rows = rows[:max_facilities]
                    logger.info(f"TEST MODE: Processing only {len(rows)} facilities")
                else:
                    logger.info(f"Processing {len(rows)} facilities")
                
                # Process facilities
                for row in rows:
                    facility_name = row['Name of the facility']
                    city = row['City']
                    urologia_rating = row['Urologia'].strip()
                    
                    # Skip empty ratings
                    if not urologia_rating:
                        logger.warning(f"Empty Urologia rating for {facility_name}, skipping")
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
                                logger.debug(f"No change needed for {facility_name}, Urologia rating already {quality_rating}")
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
                
                if test_mode:
                    # In test mode, don't actually commit changes
                    logger.info("TEST MODE: Changes not committed to database")
                    logger.info("Summary of what would be done:")
                    logger.info(f"  Updated: {stats['updated']} ratings")
                    logger.info(f"  Created: {stats['created']} new ratings")
                    session.rollback()
                else:
                    # In real mode, commit changes
                    session.commit()
                    logger.info("Changes committed to database")
        
        return stats
    
    except Exception as e:
        logger.error(f"Error importing Urologia ratings: {e}")
        # Rollback any uncommitted changes
        try:
            session.rollback()
        except:
            pass
        raise

def print_stats(stats):
    """Print statistics about the import operation"""
    print("\nImport summary:")
    print(f"  Processed:             {stats['processed']} ratings")
    print(f"  Updated:               {stats['updated']} ratings")
    print(f"  Created:               {stats['created']} new ratings")
    print(f"  Errors:                {stats['errors']} ratings")
    print(f"  Facilities not found:  {stats['facilities_not_found']} facilities")
    print(f"  Specialties not found: {stats['specialties_not_found']} specialties")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python import_urologia_ratings_test.py path/to/ratings.csv")
        sys.exit(1)
    
    # Use the Flask application context
    with app.app_context():
        csv_file = sys.argv[1]
        print(f"Testing Urologia rating import from {csv_file}...")
        
        # First run in test mode
        print("\nRunning in TEST MODE (no changes will be committed)")
        stats = import_urologia_ratings_test(csv_file, test_mode=True, max_facilities=5)
        print_stats(stats)
        
        # Ask for confirmation before running in real mode
        response = input("\nProceed with importing ALL Urologia ratings? (yes/no): ")
        if response.lower() in ['yes', 'y']:
            print("\nCreating database backup before importing...")
            backup_file = backup_database()
            print(f"Backup created: {backup_file}")
            
            print("\nRunning import for all facilities...")
            stats = import_urologia_ratings_test(csv_file, test_mode=False)
            print_stats(stats)
            print("\nUrologia ratings have been imported.")
        else:
            print("Import cancelled.")