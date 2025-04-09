"""
Update Medical Facilities with Specialty Ratings from JSON file

This script reads the rating data from the provided JSON file and updates
the medical facilities in the database with specialty-specific ratings
and strength summaries.

Usage:
  python update_from_json.py
"""

import json
import logging
import sys
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import or_, and_

from app import app, db
from models import MedicalFacility, Region, DatabaseStatus

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JSON_FILE_PATH = "attached_assets/medical_facilities_from_txt.json"

def load_json_data():
    """Load the rating data from the provided JSON file"""
    try:
        with open(JSON_FILE_PATH, 'r') as json_file:
            return json.load(json_file)
    except Exception as e:
        logger.error(f"Error loading JSON file: {str(e)}")
        return None

def update_facility_ratings(json_data):
    """
    Update facilities with ratings data from the JSON file
    
    Args:
        json_data: List of dictionaries with rating data
    
    Returns:
        dict: Statistics about the update
    """
    stats = {
        'total_processed': 0,
        'updated': 0,
        'not_found': 0,
        'errors': 0
    }
    
    try:
        for facility_data in json_data:
            stats['total_processed'] += 1
            
            # Skip entries without facility name
            if not facility_data.get("Facility") or facility_data["Facility"] == "__":
                continue
            
            facility_name = facility_data["Facility"]
            city = facility_data.get("City", "")
            region_name = facility_data.get("Region", "")
            
            # Try to find the facility by name and region/city
            query = MedicalFacility.query
            
            if region_name:
                # Join with region and filter by region name
                query = query.join(MedicalFacility.region).filter(
                    Region.name.ilike(f"%{region_name}%")
                )
            
            # Add facility name filter
            query = query.filter(MedicalFacility.name.ilike(f"%{facility_name}%"))
            
            # Add city filter if available
            if city:
                query = query.filter(
                    or_(
                        MedicalFacility.city.ilike(f"%{city}%"),
                        MedicalFacility.address.ilike(f"%{city}%")
                    )
                )
            
            facility = query.first()
            
            if facility:
                # Update specialty ratings
                update_specialty_ratings(facility, facility_data)
                stats['updated'] += 1
                
                # Log progress every 50 facilities
                if stats['updated'] % 50 == 0:
                    logger.info(f"Updated {stats['updated']} facilities so far")
                    db.session.commit()
            else:
                logger.warning(f"Facility not found: {facility_name} in {city}, {region_name}")
                stats['not_found'] += 1
        
        # Commit all changes
        db.session.commit()
        logger.info(f"Successfully updated {stats['updated']} facilities")
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error updating facility ratings: {str(e)}")
        stats['errors'] += 1
    
    return stats

def update_specialty_ratings(facility, data):
    """Update a facility with specialty ratings from the data dictionary"""
    try:
        # Update cardiology rating
        if data.get("Cardio") and data["Cardio"].isdigit():
            facility.cardiology_rating = float(data["Cardio"])
        
        # Update orthopedics rating
        if data.get("Ortho") and data["Ortho"].isdigit():
            facility.orthopedics_rating = float(data["Ortho"])
        
        # Update oncology rating
        if data.get("Onco") and data["Onco"].isdigit():
            facility.oncology_rating = float(data["Onco"])
        
        # Update neurology rating
        if data.get("Neuro") and data["Neuro"].isdigit():
            facility.neurology_rating = float(data["Neuro"])
        
        # Update surgery rating
        if data.get("Surg") and data["Surg"].isdigit():
            facility.surgery_rating = float(data["Surg"])
        
        # Update urology rating
        if data.get("Uro") and data["Uro"].isdigit():
            facility.urology_rating = float(data["Uro"])
        
        # Update pediatrics rating
        if data.get("Ped") and data["Ped"].isdigit():
            facility.pediatrics_rating = float(data["Ped"])
        
        # Update gynecology rating
        if data.get("Gyn") and data["Gyn"].isdigit():
            facility.gynecology_rating = float(data["Gyn"])
        
        # Update strengths summary
        if data.get("Strengths"):
            facility.strengths_summary = data["Strengths"]
        
    except Exception as e:
        logger.error(f"Error updating specialty ratings for {facility.name}: {str(e)}")
        raise

def update_database_status(stats):
    """Update the database status to reflect the rating updates"""
    try:
        status = DatabaseStatus.get_status()
        if status:
            notes = f"Updated {stats['updated']} facilities with specialty ratings from JSON file. {stats['not_found']} facilities not found. {stats['errors']} errors."
            
            DatabaseStatus.update_status(
                status="initialized",
                notes=notes,
                initialized_by="update_from_json.py"
            )
            logger.info("Database status updated successfully")
        else:
            logger.warning("Database status not found")
    except Exception as e:
        logger.error(f"Error updating database status: {str(e)}")

def main():
    """Main function to update facilities with specialty ratings from JSON file"""
    with app.app_context():
        logger.info("Starting update of facilities with specialty ratings from JSON file")
        
        # Load JSON data
        json_data = load_json_data()
        if not json_data:
            logger.error("Failed to load JSON data. Exiting.")
            sys.exit(1)
        
        logger.info(f"Loaded {len(json_data)} facilities from JSON file")
        
        # Update facility ratings
        stats = update_facility_ratings(json_data)
        
        # Update database status
        update_database_status(stats)
        
        # Print summary
        logger.info("=== Update Summary ===")
        logger.info(f"Total processed: {stats['total_processed']}")
        logger.info(f"Updated: {stats['updated']}")
        logger.info(f"Not found: {stats['not_found']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info("=====================")

if __name__ == "__main__":
    main()