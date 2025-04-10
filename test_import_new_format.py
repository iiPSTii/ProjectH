"""
Script di test per l'importazione di ratings da CSV con nuovo formato

Questo script testa l'importazione di ratings di specialità da un file CSV
con il nuovo formato (nome struttura, città, e punteggi per diverse specialità).
"""

import os
import csv
import logging
from sqlalchemy import text
from sqlalchemy.orm import Session
from app import app, db
from models import MedicalFacility, Specialty, FacilitySpecialty

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

def test_import_new_format(csv_file):
    """
    Test importing ratings from a CSV file with the new format.
    
    CSV format:
    Name of the facility,City,Cardiologia,Ortopedia,Oncologia,Neurologia,Urologia,Chirurgia generale,Pediatria,Ginecologia
    """
    with app.app_context():
        try:
            # Read the CSV file
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                # Statistics
                stats = {
                    'processed': 0,
                    'updated': 0,
                    'created': 0,
                    'errors': 0,
                    'facilities_not_found': 0,
                    'specialties_not_found': 0
                }
                
                # Get all specialty names from the CSV headers
                specialty_fields = [field for field in reader.fieldnames 
                                   if field not in ['Name of the facility', 'City']]
                
                print(f"Processing {len(rows)} facilities with {len(specialty_fields)} specialties")
                
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
                    
                    # Process each facility
                    for row in rows:
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
                    
                    # Commit changes
                    session.commit()
                    
                    return stats
                    
        except Exception as e:
            logger.error(f"Error testing import: {e}")
            raise

if __name__ == "__main__":
    csv_file = "sample_ratings_new_format.csv"
    
    try:
        print(f"Testing import from {csv_file}...")
        stats = test_import_new_format(csv_file)
        
        print("\nTest results:")
        print(f"  Processed:            {stats['processed']} ratings")
        print(f"  Updated:              {stats['updated']} ratings")
        print(f"  Created:              {stats['created']} new ratings")
        print(f"  Errors:               {stats['errors']} ratings")
        print(f"  Facilities not found: {stats['facilities_not_found']} facilities")
        print(f"  Specialties not found: {stats['specialties_not_found']} specialties")
        
    except Exception as e:
        print(f"Error: {e}")