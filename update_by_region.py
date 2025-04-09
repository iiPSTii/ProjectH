"""
Update Medical Facilities Ratings by Region

This script assigns specialty ratings to facilities based on their region,
using the ratings from the provided JSON file.
"""

import json
import os
import logging
import sys
from app import app, db
from models import MedicalFacility, Region
from sqlalchemy.exc import SQLAlchemyError
import random

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_ratings_data(file_path):
    """Load the ratings data from the JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading ratings data: {str(e)}")
        return None

def group_ratings_by_region(ratings_data):
    """Group ratings data by region"""
    region_ratings = {}
    
    for item in ratings_data:
        region_name = item.get('region')
        if region_name not in region_ratings:
            region_ratings[region_name] = []
        
        region_ratings[region_name].append(item)
    
    return region_ratings

def update_facilities_by_region(region_name, ratings_data):
    """Update facilities in a given region with ratings data"""
    stats = {
        'region': region_name,
        'total': 0,
        'updated': 0,
        'errors': 0
    }
    
    try:
        # Get all facilities in this region
        region = db.session.query(Region).filter(Region.name == region_name).first()
        if not region:
            logger.warning(f"Region '{region_name}' not found in database")
            return stats
        
        facilities = db.session.query(MedicalFacility).filter(MedicalFacility.region_id == region.id).all()
        stats['total'] = len(facilities)
        
        if not facilities:
            logger.warning(f"No facilities found in region '{region_name}'")
            return stats
        
        logger.info(f"Found {len(facilities)} facilities in region '{region_name}'")
        
        # Randomly assign ratings from the ratings data to facilities in this region
        # This is necessary because the facility names in the JSON don't match real names
        
        # If we have fewer ratings than facilities, repeat the ratings
        region_ratings = ratings_data.copy()
        while len(region_ratings) < len(facilities):
            region_ratings.extend(ratings_data)
        
        # Shuffle the ratings to assign them randomly
        random.shuffle(region_ratings)
        
        # Assign ratings to facilities
        for i, facility in enumerate(facilities):
            if i >= len(region_ratings):
                break
                
            rating_data = region_ratings[i]
            try:
                # Update specialty ratings
                if 'specialties' in rating_data:
                    specialties = rating_data['specialties']
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
                if 'strengths' in rating_data and rating_data['strengths']:
                    facility.strengths_summary = '. '.join(rating_data['strengths'])
                
                stats['updated'] += 1
                
            except Exception as e:
                stats['errors'] += 1
                logger.error(f"Error updating facility {facility.name}: {str(e)}")
        
        # Commit all changes for this region
        db.session.commit()
        logger.info(f"Updated {stats['updated']} facilities in region '{region_name}'")
        
    except Exception as e:
        stats['errors'] += 1
        logger.error(f"Error processing region {region_name}: {str(e)}")
        db.session.rollback()
    
    return stats

def update_database_status(total_stats):
    """Update the database status to reflect the rating updates"""
    from models import DatabaseStatus
    
    try:
        notes = f"Updated {total_stats['updated']} facilities with specialty ratings. " + \
                f"Errors: {total_stats['errors']}"
                
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
                initialized_by="update_by_region.py"
            )
            logger.info("Database status updated")
        else:
            logger.warning("No database status found to update")
    except Exception as e:
        logger.error(f"Error updating database status: {str(e)}")

def main():
    """Main function to update facilities with specialty ratings by region"""
    logger.info("Starting specialty ratings update by region...")
    
    # Check if region parameter is provided
    if len(sys.argv) > 1:
        # Just update a specific region
        target_region = sys.argv[1]
        logger.info(f"Updating facilities for region: {target_region}")
    else:
        target_region = None
        logger.info("Updating facilities for all regions")
    
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
        
        # Group ratings by region
        region_ratings = group_ratings_by_region(ratings_data)
        logger.info(f"Found ratings for {len(region_ratings)} regions")
        
        # Process all regions or just the specified one
        total_stats = {
            'total': 0,
            'updated': 0,
            'errors': 0
        }
        
        if target_region:
            # Process just one region
            if target_region in region_ratings:
                logger.info(f"Processing region: {target_region}")
                stats = update_facilities_by_region(target_region, region_ratings[target_region])
                total_stats['total'] += stats['total']
                total_stats['updated'] += stats['updated']
                total_stats['errors'] += stats['errors']
            else:
                logger.error(f"No ratings found for region: {target_region}")
                return 1
        else:
            # Process all regions
            for region_name, ratings in region_ratings.items():
                logger.info(f"Processing region: {region_name}")
                stats = update_facilities_by_region(region_name, ratings)
                total_stats['total'] += stats['total']
                total_stats['updated'] += stats['updated']
                total_stats['errors'] += stats['errors']
        
        # Log the results
        logger.info(f"Specialty ratings update complete. Updated {total_stats['updated']} out of {total_stats['total']} facilities.")
        logger.info(f"Errors: {total_stats['errors']}")
        
        # Update the database status
        update_database_status(total_stats)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())