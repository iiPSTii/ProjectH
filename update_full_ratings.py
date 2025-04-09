"""
Update Medical Facilities with Complete Specialty Ratings

This script reads the complete ratings data from a full JSON file and updates
the medical facilities in the database with specialty-specific ratings
and strength summaries.
"""

import json
import os
import logging
import sys
from app import app, db
from models import MedicalFacility, Region
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import or_

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_ratings_data(file_path):
    """Load the full ratings data from the JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading ratings data: {str(e)}")
        return None

def find_facility(name, region_name, city=None):
    """Find a facility by name and region/city"""
    # First try exact match on name and region
    query = db.session.query(MedicalFacility)
    query = query.join(MedicalFacility.region).filter(MedicalFacility.name == name)
    query = query.filter(Region.name == region_name)
    
    facility = query.first()
    if facility:
        return facility
    
    # Try with city if provided
    if city:
        query = db.session.query(MedicalFacility)
        query = query.filter(MedicalFacility.name == name)
        query = query.filter(MedicalFacility.city == city)
        facility = query.first()
        if facility:
            return facility
    
    # Try with more flexible matching
    query = db.session.query(MedicalFacility)
    query = query.join(MedicalFacility.region).filter(
        MedicalFacility.name.like(f"%{name}%"),
        Region.name == region_name
    )
    facility = query.first()
    
    return facility

def update_facility_ratings(ratings_data):
    """
    Update facilities with new comprehensive ratings data
    
    Args:
        ratings_data: List of dictionaries with rating data
    
    Returns:
        dict: Statistics about the update
    """
    stats = {
        'total': len(ratings_data),
        'updated': 0,
        'not_found': 0,
        'errors': 0
    }
    
    not_found_facilities = []
    
    # Process each facility in the ratings data
    for item in ratings_data:
        try:
            # Find the facility in the database
            facility = find_facility(item['name'], item['region'], item.get('city'))
            
            if facility:
                # Update specialty ratings
                if 'specialties' in item:
                    specialties = item['specialties']
                    if 'cardiology' in specialties:
                        facility.cardiology_rating = specialties.get('cardiology')
                    if 'orthopedics' in specialties:
                        facility.orthopedics_rating = specialties.get('orthopedics')
                    if 'oncology' in specialties:
                        facility.oncology_rating = specialties.get('oncology')
                    if 'neurology' in specialties:
                        facility.neurology_rating = specialties.get('neurology')
                    if 'surgery' in specialties:
                        facility.surgery_rating = specialties.get('surgery')
                    if 'urology' in specialties:
                        facility.urology_rating = specialties.get('urology')
                    if 'pediatrics' in specialties:
                        facility.pediatrics_rating = specialties.get('pediatrics')
                    if 'gynecology' in specialties:
                        facility.gynecology_rating = specialties.get('gynecology')
                
                # Update strengths summary
                if 'strengths' in item and item['strengths']:
                    facility.strengths_summary = '. '.join(item['strengths'])
                
                stats['updated'] += 1
                
                # Commit after each update to avoid long transactions
                db.session.commit()
                
                # Log progress for every 100 facilities
                if stats['updated'] % 100 == 0:
                    logger.info(f"Updated {stats['updated']} facilities so far")
            else:
                stats['not_found'] += 1
                not_found_facilities.append(item['name'])
        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Error updating facility {item.get('name', '')}: {str(e)}")
            db.session.rollback()
    
    # Log facilities that weren't found if there aren't too many
    if stats['not_found'] <= 10:
        logger.warning(f"Facilities not found: {', '.join(not_found_facilities)}")
    else:
        logger.warning(f"{stats['not_found']} facilities were not found in the database")
    
    return stats

def update_database_status(stats):
    """Update the database status to reflect the rating updates"""
    from models import DatabaseStatus
    
    try:
        notes = f"Updated {stats['updated']} facilities with specialty ratings from the full dataset. " + \
                f"Not found: {stats['not_found']}, Errors: {stats['errors']}"
                
        status = DatabaseStatus.get_status()
        if status:
            total_facilities = status.total_facilities
            total_regions = status.total_regions
            total_specialties = status.total_specialties
            
            DatabaseStatus.update_status(
                status="initialized",
                total_facilities=total_facilities,
                total_regions=total_regions,
                total_specialties=total_specialties,
                notes=notes,
                initialized_by="update_full_ratings.py"
            )
            logger.info("Database status updated")
        else:
            logger.warning("No database status found to update")
    except Exception as e:
        logger.error(f"Error updating database status: {str(e)}")

def main():
    """Main function to update facilities with specialty ratings"""
    logger.info("Starting full specialty ratings update...")
    
    # Check if batch parameter is provided
    if len(sys.argv) > 1:
        try:
            batch = int(sys.argv[1])
        except ValueError:
            logger.error("Batch parameter must be an integer")
            return 1
    else:
        batch = 0
    
    with app.app_context():
        # Check if the database is initialized
        from models import DatabaseStatus
        status = DatabaseStatus.get_status()
        
        if not status or status.status != "initialized":
            logger.error("Database not initialized. Please run initialize_database.py first.")
            return 1
        
        # Load the ratings data
        file_path = "attached_assets/true_full_italian_medical_facilities.json"
        
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"Ratings file not found at {file_path}")
            return 1
        
        ratings_data = load_ratings_data(file_path)
        if not ratings_data:
            logger.error("Failed to load ratings data")
            return 1
        
        # Process in batches of 50 to avoid timeouts
        batch_size = 50
        total_batches = (len(ratings_data) + batch_size - 1) // batch_size
        
        if batch >= total_batches:
            logger.error(f"Invalid batch number. There are only {total_batches} batches (0-{total_batches-1}).")
            return 1
        
        start_idx = batch * batch_size
        end_idx = min(start_idx + batch_size, len(ratings_data))
        
        logger.info(f"Processing batch {batch+1}/{total_batches} (facilities {start_idx}-{end_idx-1})")
        batch_data = ratings_data[start_idx:end_idx]
        
        # Update the facilities in this batch
        stats = update_facility_ratings(batch_data)
        
        # Log the results
        logger.info(f"Batch {batch+1}/{total_batches} complete. Updated {stats['updated']} out of {stats['total']} facilities.")
        logger.info(f"Not found: {stats['not_found']}, Errors: {stats['errors']}")
        
        # Update the database status
        update_database_status(stats)
        
        # Provide info about continuing the process
        if batch < total_batches - 1:
            next_batch = batch + 1
            logger.info(f"To continue with the next batch, run: python update_full_ratings.py {next_batch}")
        else:
            logger.info("All batches have been processed!")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())