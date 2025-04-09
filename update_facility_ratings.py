"""
Update Medical Facilities with Specialty Ratings

This script reads the enriched ratings data from a JSON file and updates
the medical facilities in the database with specialty-specific ratings
and strength summaries.
"""

import json
import os
import logging
from app import app, db
from models import MedicalFacility, DatabaseStatus
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Column, Float, String, text

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# JSON file path
RATINGS_FILE = 'attached_assets/italian_medical_facilities_ratings.json'

def load_ratings_data():
    """Load the ratings data from the JSON file"""
    try:
        with open(RATINGS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading ratings data: {e}")
        return None

def verify_rating_columns():
    """Verify that the rating columns exist in the MedicalFacility model"""
    try:
        # Get the model class attributes
        model_attrs = dir(MedicalFacility)
        
        # Columns that should be in the model
        required_columns = [
            'cardiology_rating',
            'orthopedics_rating',
            'oncology_rating',
            'neurology_rating',
            'surgery_rating',
            'urology_rating',
            'pediatrics_rating',
            'gynecology_rating',
            'strengths_summary'
        ]
        
        # Check if all required columns are in the model
        missing_columns = [col for col in required_columns if col not in model_attrs]
        
        if missing_columns:
            logger.error(f"Missing columns in MedicalFacility model: {missing_columns}")
            return False
        
        logger.info("All rating columns are present in the model")
        return True
    except Exception as e:
        logger.error(f"Error verifying rating columns: {e}")
        return False

def update_facility_ratings(ratings_data):
    """
    Update facilities with ratings data
    
    Args:
        ratings_data: List of dictionaries with rating data
    
    Returns:
        dict: Statistics about the update
    """
    stats = {
        'total_ratings': len(ratings_data),
        'matched': 0,
        'not_found': 0,
        'errors': 0
    }
    
    for rating in ratings_data:
        try:
            # Try to find the facility by name and region/city
            facility_name = rating['facility']
            region_name = rating['region']
            city_name = rating['city']
            
            # Log the facility we're trying to match
            logger.info(f"Looking for facility: '{facility_name}' in {city_name}, {region_name}")
            
            # Get facilities that match the name exactly or with LIKE
            query = MedicalFacility.query.filter(
                db.or_(
                    MedicalFacility.name == facility_name,
                    MedicalFacility.name.ilike(f"%{facility_name}%")
                )
            )
            
            # Further filter by region and city
            candidates = []
            for facility in query.all():
                # Check for region match if we have region data
                region_match = False
                if facility.region and region_name:
                    if region_name.lower() in facility.region.name.lower():
                        region_match = True
                
                # Check for city match if we have city data
                city_match = False
                if facility.city and city_name:
                    if city_name.lower() in facility.city.lower():
                        city_match = True
                
                # Add to candidates if it matches region or city
                if region_match or city_match:
                    candidates.append(facility)
            
            # If we found matching facilities, update them
            if candidates:
                logger.info(f"Found {len(candidates)} matches for {facility_name}")
                for facility in candidates:
                    # Update specialty ratings - log before and after values
                    logger.info(f"Updating {facility.name} (ID: {facility.id}) ratings:")
                    logger.info(f"  Before - Cardiology: {facility.cardiology_rating}, Oncology: {facility.oncology_rating}")
                    
                    # Direct SQL update to ensure it's working
                    db.session.execute(
                        text(f"""
                        UPDATE medical_facilities SET 
                            cardiology_rating = :cardiology,
                            orthopedics_rating = :orthopedics,
                            oncology_rating = :oncology,
                            neurology_rating = :neurology,
                            surgery_rating = :surgery,
                            urology_rating = :urology,
                            pediatrics_rating = :pediatrics,
                            gynecology_rating = :gynecology,
                            strengths_summary = :summary
                        WHERE id = :id
                        """),
                        {
                            'cardiology': float(rating.get('cardiology_rating')),
                            'orthopedics': float(rating.get('orthopedics_rating')),
                            'oncology': float(rating.get('oncology_rating')),
                            'neurology': float(rating.get('neurology_rating')),
                            'surgery': float(rating.get('surgery_rating')),
                            'urology': float(rating.get('urology_rating')),
                            'pediatrics': float(rating.get('pediatrics_rating')),
                            'gynecology': float(rating.get('gynecology_rating')),
                            'summary': rating.get('strengths_summary', ''),
                            'id': facility.id
                        }
                    )
                    
                    # Also update using the ORM approach
                    facility.cardiology_rating = float(rating.get('cardiology_rating'))
                    facility.orthopedics_rating = float(rating.get('orthopedics_rating'))
                    facility.oncology_rating = float(rating.get('oncology_rating'))
                    facility.neurology_rating = float(rating.get('neurology_rating'))
                    facility.surgery_rating = float(rating.get('surgery_rating'))
                    facility.urology_rating = float(rating.get('urology_rating'))
                    facility.pediatrics_rating = float(rating.get('pediatrics_rating'))
                    facility.gynecology_rating = float(rating.get('gynecology_rating'))
                    facility.strengths_summary = rating.get('strengths_summary', '')
                    
                    # Refresh the facility from the database
                    db.session.flush()
                    db.session.refresh(facility)
                    
                    logger.info(f"  After - Cardiology: {facility.cardiology_rating}, Oncology: {facility.oncology_rating}")
                    
                    # DO NOT recalculate overall quality score as average of specialty ratings
                    # The original quality score is preserved as a "general" rating
                    # that is independent from specialty-specific ratings
                    
                    # Log that we're keeping the original quality score
                    logger.info(f"  Keeping original quality score: {facility.quality_score}")
                    
                stats['matched'] += 1
            else:
                logger.warning(f"No match found for {facility_name} in {region_name}, {city_name}")
                stats['not_found'] += 1
                
        except Exception as e:
            logger.error(f"Error updating facility {rating.get('facility')}: {e}")
            stats['errors'] += 1
    
    # Commit all changes
    try:
        db.session.commit()
        logger.info(f"Successfully updated {stats['matched']} facilities with specialty ratings")
    except SQLAlchemyError as e:
        logger.error(f"Error committing changes: {e}")
        db.session.rollback()
        stats['errors'] += stats['matched']
        stats['matched'] = 0
    
    return stats

def update_database_status(stats):
    """Update the database status to reflect the rating updates"""
    try:
        current_status = DatabaseStatus.get_status()
        
        if current_status:
            status_notes = (
                f"Updated {stats['matched']} facilities with specialty ratings. "
                f"({stats['not_found']} facilities not found, {stats['errors']} errors)"
            )
            
            # Create new status record
            DatabaseStatus.update_status(
                status="initialized",
                total_facilities=current_status.total_facilities,
                total_regions=current_status.total_regions,
                total_specialties=current_status.total_specialties,
                notes=status_notes,
                initialized_by="update_facility_ratings.py"
            )
            
            logger.info("Database status updated successfully")
            return True
        else:
            logger.warning("No current database status found, skipping status update")
            return False
    except Exception as e:
        logger.error(f"Error updating database status: {e}")
        return False

def main():
    """Main function to update facilities with specialty ratings"""
    logger.info("Starting facility ratings update")
    
    with app.app_context():
        # Load ratings data
        ratings_data = load_ratings_data()
        
        if not ratings_data:
            logger.error("Failed to load ratings data, aborting")
            return False
            
        logger.info(f"Loaded {len(ratings_data)} facility ratings")
        
        # Verify that rating columns exist in the model
        if not verify_rating_columns():
            logger.error("Rating columns not properly defined in the model, aborting")
            return False
        
        # Update facilities with ratings
        stats = update_facility_ratings(ratings_data)
        
        # Update database status
        update_database_status(stats)
        
        logger.info("Facility ratings update completed successfully")
        print("Updated dataset saved successfully.")
        
        return True

if __name__ == "__main__":
    main()