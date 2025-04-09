"""
Geocode Facilities Script

This script geocodes all medical facilities in the database and
stores their coordinates for future address search operations.
This is done once upfront to avoid repeated geocoding during searches.
"""

import time
import logging
from sqlalchemy import func
from app import app, db
from models import MedicalFacility
from geocoding import extract_coordinates_from_address

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
BATCH_SIZE = 5  # Process facilities in batches to avoid timeouts
GEOCODING_DELAY = 0.5  # Delay between geocoding requests to respect API limits

def geocode_facilities(batch_size=None, max_facilities=None):
    """
    Geocode medical facilities in the database and store their coordinates.
    
    Args:
        batch_size (int, optional): Process facilities in batches of this size.
            Defaults to BATCH_SIZE constant.
        max_facilities (int, optional): Maximum number of facilities to process in total.
            Useful for web requests to avoid timeouts. If None, process all.
    """
    with app.app_context():
        # Get count of facilities that need geocoding
        query = db.session.query(MedicalFacility)\
            .filter(MedicalFacility.geocoded == False)\
            .order_by(MedicalFacility.id)
        
        # Limit the query if max_facilities is specified
        if max_facilities:
            query = query.limit(max_facilities)
        
        facilities_to_geocode = query.all()
        
        # Use the specified batch size or default
        actual_batch_size = batch_size or BATCH_SIZE
        
        total_facilities = len(facilities_to_geocode)
        logger.info(f"Found {total_facilities} facilities that need geocoding.")
        
        if total_facilities == 0:
            logger.info("No facilities need geocoding. Exiting.")
            return
        
        # Process facilities in batches
        total_processed = 0
        total_success = 0
        
        for i in range(0, total_facilities, actual_batch_size):
            batch = facilities_to_geocode[i:i+actual_batch_size]
            logger.info(f"Processing batch {i//actual_batch_size + 1}/{(total_facilities+actual_batch_size-1)//actual_batch_size} ({len(batch)} facilities)")
            
            for facility in batch:
                try:
                    # Skip facilities that already have coordinates
                    if facility.latitude is not None and facility.longitude is not None:
                        facility.geocoded = True
                        db.session.commit()
                        logger.info(f"Facility already has coordinates: {facility.name}")
                        total_processed += 1
                        total_success += 1
                        continue
                    
                    # Geocode the facility address
                    coords = extract_coordinates_from_address(facility.address, facility.city)
                    
                    if coords:
                        # Update facility with coordinates
                        facility.latitude = coords['lat']
                        facility.longitude = coords['lon']
                        facility.geocoded = True
                        db.session.commit()
                        
                        logger.info(f"Successfully geocoded: {facility.name} ({facility.address}, {facility.city})")
                        total_success += 1
                    else:
                        logger.warning(f"Failed to geocode: {facility.name} ({facility.address}, {facility.city})")
                        # Mark as geocoded but with no coordinates to avoid repeated attempts
                        facility.geocoded = True
                        db.session.commit()
                    
                    # Respect rate limits
                    time.sleep(GEOCODING_DELAY)
                    
                except Exception as e:
                    logger.error(f"Error geocoding facility {facility.name}: {str(e)}")
                    # Continue with the next facility
                    continue
                
                total_processed += 1
                
                # Log progress
                if total_processed % 10 == 0 or total_processed == total_facilities:
                    success_rate = (total_success / total_processed) * 100 if total_processed > 0 else 0
                    logger.info(f"Progress: {total_processed}/{total_facilities} facilities processed ({success_rate:.1f}% success rate)")
            
            logger.info(f"Completed batch {i//actual_batch_size + 1}/{(total_facilities+actual_batch_size-1)//actual_batch_size}")
        
        logger.info(f"Geocoding complete. {total_success}/{total_facilities} facilities successfully geocoded.")
        
        # Return stats for web interface
        return {
            "total_processed": total_processed,
            "total_success": total_success
        }

def update_all_facilities_geocoded_status():
    """
    Set geocoded=False for all facilities to force re-geocoding.
    Use this if you need to refresh all coordinates.
    """
    with app.app_context():
        db.session.query(MedicalFacility).update({"geocoded": False})
        db.session.commit()
        logger.info("Reset geocoded status for all facilities.")

def get_geocoding_statistics():
    """
    Get statistics about the geocoding status of facilities.
    """
    with app.app_context():
        total_facilities = db.session.query(func.count(MedicalFacility.id)).scalar()
        geocoded_facilities = db.session.query(func.count(MedicalFacility.id))\
            .filter(MedicalFacility.geocoded == True).scalar()
        facilities_with_coords = db.session.query(func.count(MedicalFacility.id))\
            .filter(MedicalFacility.latitude != None)\
            .filter(MedicalFacility.longitude != None).scalar()
        
        logger.info(f"Total facilities: {total_facilities}")
        logger.info(f"Geocoded facilities: {geocoded_facilities} ({(geocoded_facilities/total_facilities*100):.1f}%)")
        logger.info(f"Facilities with coordinates: {facilities_with_coords} ({(facilities_with_coords/total_facilities*100):.1f}%)")
        
        return {
            "total": total_facilities,
            "geocoded": geocoded_facilities,
            "with_coords": facilities_with_coords
        }

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--reset":
        update_all_facilities_geocoded_status()
    
    get_geocoding_statistics()
    geocode_facilities()
    get_geocoding_statistics()