"""
Update Specialty Ratings Script

This script updates the specialty ratings for medical facilities from a CSV file.
It first creates a backup of the database, then processes the CSV file to update
the FacilitySpecialty junction table with new ratings.

CSV format should have the following columns:
- facility_id: ID of the facility
- specialty_id: ID of the specialty (or specialty_name: Name of the specialty)
- quality_rating: New rating value (1-5 scale)

Usage:
  python update_specialty_ratings.py path/to/ratings.csv
"""

import sys
import csv
import logging
from datetime import datetime
from sqlalchemy.orm import Session
from app import db
from models import MedicalFacility, Specialty, FacilitySpecialty
from backup_database import backup_database

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_specialty_ratings(csv_file):
    """
    Update specialty ratings from a CSV file
    
    Args:
        csv_file: Path to the CSV file with updated ratings
    
    Returns:
        dict: Statistics about the update operation
    """
    stats = {
        'processed': 0,
        'updated': 0,
        'created': 0,
        'errors': 0,
        'unchanged': 0
    }
    
    try:
        # First, back up the database
        logger.info("Creating database backup...")
        backup_file = backup_database()
        logger.info(f"Backup created: {backup_file}")
        
        # Now process the CSV file
        logger.info(f"Processing ratings from {csv_file}...")
        
        # Get specialty mapping for lookup by name if needed
        with Session(db.engine) as session:
            specialty_map = {s.name.lower(): s.id for s in session.query(Specialty).all()}
            
            # Read and process the CSV file
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                # Validate CSV structure
                required_fields = ["facility_id"]
                if not all(field in reader.fieldnames for field in required_fields):
                    missing = [f for f in required_fields if f not in reader.fieldnames]
                    raise ValueError(f"CSV is missing required fields: {', '.join(missing)}")
                
                # Check if we have specialty_id or specialty_name
                if "specialty_id" not in reader.fieldnames and "specialty_name" not in reader.fieldnames:
                    raise ValueError("CSV must contain either 'specialty_id' or 'specialty_name' column")
                
                if "quality_rating" not in reader.fieldnames:
                    raise ValueError("CSV must contain 'quality_rating' column")
                
                # Process in batches to avoid memory issues
                batch_size = 500
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i+batch_size]
                    process_batch(session, batch, specialty_map, stats)
                    session.commit()
                    logger.info(f"Processed batch {i//batch_size + 1}, {stats['processed']} rows so far")
        
        # Update the database status to reflect the changes
        update_database_status(f"Specialty ratings updated from {csv_file}")
        
        return stats
    
    except Exception as e:
        logger.error(f"Error updating specialty ratings: {e}")
        # Rollback any uncommitted changes
        try:
            session.rollback()
        except:
            pass
        raise

def process_batch(session, batch, specialty_map, stats):
    """Process a batch of rows from the CSV file"""
    for row in batch:
        stats['processed'] += 1
        
        try:
            # Get facility and specialty IDs
            facility_id = int(row['facility_id'])
            
            if 'specialty_id' in row and row['specialty_id']:
                specialty_id = int(row['specialty_id'])
            elif 'specialty_name' in row and row['specialty_name']:
                specialty_name = row['specialty_name'].lower()
                if specialty_name in specialty_map:
                    specialty_id = specialty_map[specialty_name]
                else:
                    logger.warning(f"Unknown specialty name: {row['specialty_name']}")
                    stats['errors'] += 1
                    continue
            else:
                logger.warning(f"Row {stats['processed']} has no specialty identification")
                stats['errors'] += 1
                continue
            
            # Convert rating to float
            try:
                quality_rating = float(row['quality_rating'])
                # Ensure rating is within valid range
                quality_rating = max(0.0, min(5.0, quality_rating))
            except (ValueError, TypeError):
                logger.warning(f"Invalid quality rating: {row['quality_rating']}")
                stats['errors'] += 1
                continue
            
            # Check if facility-specialty relationship exists
            facility_specialty = session.query(FacilitySpecialty).filter_by(
                facility_id=facility_id, 
                specialty_id=specialty_id
            ).first()
            
            if facility_specialty:
                # Update existing relationship if the rating is different
                if abs(facility_specialty.quality_rating - quality_rating) > 0.001:
                    old_rating = facility_specialty.quality_rating
                    facility_specialty.quality_rating = quality_rating
                    logger.debug(f"Updated rating for facility {facility_id}, specialty {specialty_id}: {old_rating} -> {quality_rating}")
                    stats['updated'] += 1
                else:
                    stats['unchanged'] += 1
            else:
                # Create new relationship
                # First verify facility and specialty exist
                facility = session.query(MedicalFacility).get(facility_id)
                specialty = session.query(Specialty).get(specialty_id)
                
                if not facility:
                    logger.warning(f"Facility with ID {facility_id} does not exist")
                    stats['errors'] += 1
                    continue
                
                if not specialty:
                    logger.warning(f"Specialty with ID {specialty_id} does not exist")
                    stats['errors'] += 1
                    continue
                
                # Create new relationship
                new_relationship = FacilitySpecialty(
                    facility_id=facility_id,
                    specialty_id=specialty_id,
                    quality_rating=quality_rating
                )
                session.add(new_relationship)
                logger.debug(f"Created new rating for facility {facility_id}, specialty {specialty_id}: {quality_rating}")
                stats['created'] += 1
        
        except Exception as e:
            logger.error(f"Error processing row {stats['processed']}: {e}")
            stats['errors'] += 1

def update_database_status(message):
    """Update the database status to reflect the update operation"""
    from app import get_database_status
    from sqlalchemy import text
    
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
        print("Usage: python update_specialty_ratings.py path/to/ratings.csv")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    print(f"Updating specialty ratings from {csv_file}...")
    stats = update_specialty_ratings(csv_file)
    
    print("\nUpdate completed:")
    print(f"  Processed: {stats['processed']} rows")
    print(f"  Updated:   {stats['updated']} ratings")
    print(f"  Created:   {stats['created']} new ratings")
    print(f"  Unchanged: {stats['unchanged']} ratings")
    print(f"  Errors:    {stats['errors']} rows")