"""
Update Medical Facility Database with Specializations and Ratings

This script updates the existing PostgreSQL database with new columns for specialty ratings
and strength summaries from the provided enriched dataset.
"""

import os
import json
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add the current directory to the Python path to import our models
sys.path.append('.')

# Import after setting up the path
from app import app, db
from models import MedicalFacility, Region

# Load the enriched data from the JSON file
def load_enriched_data(json_file_path):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Loaded {len(data)} facilities from enriched dataset")
        return data
    except Exception as e:
        logger.error(f"Error loading enriched data: {str(e)}")
        return []

# Function to update a facility object with data from the enriched dataset
def update_facility_from_enriched_data(facility, facility_data):
    """
    Update a facility object with data from the enriched dataset
    """
    facility.cardiology_rating = facility_data.get('cardiology_rating')
    facility.orthopedics_rating = facility_data.get('orthopedics_rating')
    facility.oncology_rating = facility_data.get('oncology_rating')
    facility.neurology_rating = facility_data.get('neurology_rating')
    facility.surgery_rating = facility_data.get('surgery_rating')
    facility.urology_rating = facility_data.get('urology_rating')
    facility.pediatrics_rating = facility_data.get('pediatrics_rating')
    facility.gynecology_rating = facility_data.get('gynecology_rating')
    facility.strengths_summary = facility_data.get('strengths_summary', '')
    return facility

def main():
    try:
        # Path to the enriched data JSON file
        json_file_path = "attached_assets/italian_medical_facilities_ratings.json"
        
        # Load the enriched data
        enriched_data = load_enriched_data(json_file_path)
        if not enriched_data:
            logger.error("No enriched data available to update the database")
            return False
        
        # Use Flask app context to access the database
        with app.app_context():
            # Create the tables if they don't exist
            logger.info("Creating or updating database tables")
            db.create_all()
            
            matches = 0
            
            # Process each facility in the enriched data
            for facility_data in enriched_data:
                facility_name = facility_data['facility']
                region_name = facility_data.get('region')
                city_name = facility_data.get('city')
                
                # Look for matching facilities using SQLAlchemy ORM
                query = MedicalFacility.query.filter(MedicalFacility.name.ilike(f"%{facility_name}%"))
                
                if region_name:
                    # Join with regions table if region specified
                    region = Region.query.filter(Region.name.ilike(f"%{region_name}%")).first()
                    if region:
                        query = query.filter(MedicalFacility.region_id == region.id)
                elif city_name:
                    # Filter by city if region not available
                    query = query.filter(MedicalFacility.city.ilike(f"%{city_name}%"))
                
                # Get all matching facilities
                facilities = query.all()
                
                if facilities:
                    matches += 1
                    # Update each matching facility
                    for facility in facilities:
                        # Update the facility object with enriched data
                        update_facility_from_enriched_data(facility, facility_data)
                        logger.info(f"Updated facility: {facility.name} (ID: {facility.id})")
                    
                    # Commit the changes to the database
                    db.session.commit()
                else:
                    logger.warning(f"No match found for facility: {facility_name} in {region_name or city_name or 'unknown location'}")
            
            logger.info(f"Matched and updated {matches} facilities out of {len(enriched_data)} in the enriched dataset")
            print(f"✅ Updated dataset saved successfully. Updated {matches} facilities.")
            
            # Update database status to reflect the changes
            from models import DatabaseStatus
            current_status = DatabaseStatus.get_status()
            if current_status:
                DatabaseStatus.update_status(
                    status="updated",
                    total_facilities=current_status.total_facilities,
                    total_regions=current_status.total_regions,
                    total_specialties=current_status.total_specialties,
                    notes="Updated with specialty ratings and strength summaries"
                )
                logger.info("Database status updated")
            
            return True
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        return False

if __name__ == "__main__":
    main()