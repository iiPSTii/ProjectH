"""
Geocode a small batch of facilities

This script geocodes a small batch of facilities with missing coordinates
and can be run multiple times to make progress without hitting timeouts.
"""

import logging
import sys
import time
import random
import requests

from app import app, db
from models import MedicalFacility

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('geocoding.log')
    ]
)
logger = logging.getLogger(__name__)

# Constants
API_ENDPOINT = "https://nominatim.openstreetmap.org/search"
SLEEP_BETWEEN_REQUESTS = 3  # seconds to wait between API requests to avoid rate limiting
BATCH_SIZE = 3  # number of facilities to process per run
USER_AGENT = "FindMyCureItalia/1.0"

def get_stats():
    """Get geocoding statistics"""
    total = db.session.query(MedicalFacility).count()
    geocoded = db.session.query(MedicalFacility).filter(MedicalFacility.geocoded == True).count()
    with_coords = db.session.query(MedicalFacility).filter(
        MedicalFacility.latitude != None,
        MedicalFacility.longitude != None
    ).count()
    missing = db.session.query(MedicalFacility).filter(
        MedicalFacility.geocoded == True,
        MedicalFacility.latitude.is_(None)
    ).count()
    
    logger.info(f"Total facilities: {total}")
    logger.info(f"Geocoded facilities: {geocoded} ({geocoded/total*100:.1f}%)")
    logger.info(f"Facilities with coordinates: {with_coords} ({with_coords/total*100:.1f}%)")
    logger.info(f"Missing coordinates: {missing}")
    
    return {
        'total': total,
        'geocoded': geocoded,
        'with_coords': with_coords,
        'missing': missing
    }

def get_next_batch():
    """Get the next batch of facilities that need coordinates"""
    facilities = db.session.query(MedicalFacility).filter(
        MedicalFacility.geocoded == True,
        MedicalFacility.latitude.is_(None)
    ).limit(BATCH_SIZE).all()
    
    return facilities

def geocode_address(address):
    """Geocode an address using Nominatim"""
    headers = {
        'User-Agent': USER_AGENT,
        'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7'
    }
    
    params = {
        'q': address,
        'format': 'json',
        'limit': 1,
        'countrycodes': 'it'
    }
    
    try:
        response = requests.get(API_ENDPOINT, params=params, headers=headers)
        
        if response.status_code == 200:
            results = response.json()
            
            if results and len(results) > 0:
                lat = float(results[0]['lat'])
                lon = float(results[0]['lon'])
                return {'lat': lat, 'lon': lon, 'display_name': results[0]['display_name']}
        
        return None
    except Exception as e:
        logger.error(f"Error geocoding '{address}': {str(e)}")
        return None

def update_coordinates(facility, coords):
    """Update a facility's coordinates"""
    try:
        facility.latitude = coords['lat']
        facility.longitude = coords['lon']
        db.session.commit()
        return True
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error updating facility {facility.id}: {str(e)}")
        return False

def process_facility(facility):
    """Process a single facility"""
    # Generate various address formats to try
    addresses = []
    
    # Try with original address fields
    if facility.address and facility.city:
        addresses.append(f"{facility.address}, {facility.city}, Italy")
    
    if facility.address:
        addresses.append(f"{facility.address}, Italy")
    
    if facility.city:
        addresses.append(f"{facility.city}, Italy")
    
    if facility.region:
        addresses.append(f"{facility.name}, {facility.region.name}, Italy")
        addresses.append(f"{facility.region.name}, Italy")
    
    # Try different formats without success
    for address in addresses:
        logger.info(f"Trying address: {address}")
        
        coords = geocode_address(address)
        
        if coords:
            logger.info(f"Found coordinates for '{address}': {coords['display_name']}")
            return update_coordinates(facility, coords)
        
        # Wait before trying the next format
        sleep_time = SLEEP_BETWEEN_REQUESTS + random.uniform(0.5, 1.5)
        logger.info(f"Waiting {sleep_time:.1f} seconds before next attempt...")
        time.sleep(sleep_time)
    
    logger.warning(f"Failed to geocode facility {facility.id}: {facility.name}")
    return False

def main():
    """Main function"""
    with app.app_context():
        # Get statistics before processing
        logger.info("Getting statistics before processing...")
        stats_before = get_stats()
        
        if stats_before['missing'] == 0:
            logger.info("No facilities with missing coordinates found!")
            return 0
        
        # Get the next batch of facilities
        facilities = get_next_batch()
        logger.info(f"Processing batch of {len(facilities)} facilities")
        
        success_count = 0
        
        for i, facility in enumerate(facilities, 1):
            logger.info(f"Processing {i}/{len(facilities)}: {facility.name}")
            
            if process_facility(facility):
                success_count += 1
            
            # Wait before next facility
            if i < len(facilities):
                sleep_time = SLEEP_BETWEEN_REQUESTS * 2
                logger.info(f"Waiting {sleep_time} seconds before next facility...")
                time.sleep(sleep_time)
        
        # Get statistics after processing
        logger.info("Getting statistics after processing...")
        stats_after = get_stats()
        
        logger.info(f"Processed {len(facilities)} facilities, fixed {success_count}")
        logger.info(f"Remaining facilities with missing coordinates: {stats_after['missing']}")
        
        return 0

if __name__ == "__main__":
    sys.exit(main())