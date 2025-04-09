"""
Fix Missing Coordinates Script

This script identifies facilities that are marked as geocoded but are missing
latitude and longitude coordinates, and attempts to re-geocode them using a
more flexible approach.
"""

import argparse
import logging
import time
import random
from urllib.parse import quote

import requests
from sqlalchemy import and_

from app import app, db
from models import MedicalFacility
from geocoding import extract_coordinates_from_address

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
MAX_RETRIES = 3

# User agent for the requests
USER_AGENT = "FindMyCureItalia/1.0 (https://example.com; contact@example.com)"

def get_facilities_missing_coordinates():
    """Get facilities that are marked as geocoded but are missing coordinates"""
    facilities = db.session.query(MedicalFacility).filter(
        and_(
            MedicalFacility.geocoded == True,
            MedicalFacility.latitude.is_(None)
        )
    ).all()
    return facilities

def geocode_with_fallback(facility):
    """
    Attempt to geocode a facility with various fallback strategies
    
    Strategies:
    1. Try with full address including city
    2. Try with city name only
    3. Try with a more generic search using region name
    """
    if facility.address and facility.city:
        address = f"{facility.address}, {facility.city}, Italy"
    elif facility.address:
        address = f"{facility.address}, Italy"
    elif facility.city:
        address = f"{facility.city}, Italy"
    else:
        # If no address or city, try with facility name and region
        region_name = facility.region.name if facility.region else "Italy"
        address = f"{facility.name}, {region_name}, Italy"
    
    logger.info(f"Trying to geocode facility {facility.id}: {facility.name} using address: {address}")
    
    # Strategy 1: Try with full address
    coords = get_coordinates(address)
    if coords:
        return coords
    
    # Strategy 2: Try with city only if we have it
    if facility.city:
        logger.info(f"Trying fallback with city only: {facility.city}, Italy")
        coords = get_coordinates(f"{facility.city}, Italy")
        if coords:
            return coords
    
    # Strategy 3: Try with region
    if facility.region:
        logger.info(f"Trying fallback with region: {facility.region.name}, Italy")
        coords = get_coordinates(f"{facility.region.name}, Italy")
        if coords:
            return coords
    
    # If all strategies failed, return None
    return None

def get_coordinates(address):
    """
    Get coordinates for an address using the Nominatim API
    
    Args:
        address: The address to geocode
        
    Returns:
        dict: Coordinates with lat and lon keys, or None if geocoding failed
    """
    for attempt in range(MAX_RETRIES):
        try:
            # Prepare the API request
            params = {
                'q': address,
                'format': 'json',
                'limit': 1,
                'countrycodes': 'it'
            }
            
            # Add proper headers to avoid 403 errors
            headers = {
                'User-Agent': USER_AGENT,
                'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
                'Referer': 'https://findmycure.example.com/'
            }
            
            # Send the request with headers
            response = requests.get(API_ENDPOINT, params=params, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                results = response.json()
                
                # Check if any results were returned
                if results and len(results) > 0:
                    # Extract the coordinates
                    lat = float(results[0]['lat'])
                    lon = float(results[0]['lon'])
                    
                    logger.info(f"Successfully geocoded address: {address} -> {results[0]['display_name']}")
                    
                    return {'lat': lat, 'lon': lon}
                else:
                    logger.warning(f"No results found for address: {address}")
            else:
                logger.warning(f"API request failed with status code {response.status_code} for address: {address}")
                
            # Wait before retrying with a random delay to avoid rate limiting
            sleep_time = SLEEP_BETWEEN_REQUESTS + random.uniform(1, 2)
            logger.info(f"Waiting {sleep_time:.1f} seconds before next request...")
            time.sleep(sleep_time)
            
        except Exception as e:
            logger.error(f"Error geocoding address '{address}': {str(e)}")
            time.sleep(SLEEP_BETWEEN_REQUESTS)
    
    return None

def update_facility_coordinates(facility, coordinates):
    """Update a facility's coordinates in the database"""
    try:
        facility.latitude = coordinates['lat']
        facility.longitude = coordinates['lon']
        db.session.commit()
        logger.info(f"Updated coordinates for facility {facility.id}: {facility.name}")
        return True
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error updating coordinates for facility {facility.id}: {str(e)}")
        return False

def fix_missing_coordinates(max_facilities=None):
    """
    Fix facilities with missing coordinates
    
    Args:
        max_facilities: Maximum number of facilities to process (default: all)
    """
    with app.app_context():
        facilities = get_facilities_missing_coordinates()
        total = len(facilities)
        
        logger.info(f"Found {total} facilities missing coordinates")
        
        if max_facilities:
            facilities = facilities[:max_facilities]
            logger.info(f"Processing first {len(facilities)} facilities")
        
        success_count = 0
        
        # Process facilities in smaller batches to avoid overwhelming the API
        batch_size = 5
        for batch_start in range(0, len(facilities), batch_size):
            batch_end = min(batch_start + batch_size, len(facilities))
            current_batch = facilities[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_start//batch_size + 1}/{(len(facilities)-1)//batch_size + 1} ({len(current_batch)} facilities)")
            
            for i, facility in enumerate(current_batch, 1):
                logger.info(f"Processing facility {batch_start + i}/{len(facilities)}: {facility.name}")
                
                # Try to geocode with fallback strategies
                coords = geocode_with_fallback(facility)
                
                if coords:
                    if update_facility_coordinates(facility, coords):
                        success_count += 1
                else:
                    logger.warning(f"Failed to geocode facility {facility.id}: {facility.name}")
                
                # Avoid overloading the API
                if i < len(current_batch):
                    sleep_time = SLEEP_BETWEEN_REQUESTS + random.uniform(0.5, 1.5)
                    logger.info(f"Waiting {sleep_time:.1f} seconds before next facility...")
                    time.sleep(sleep_time)
            
            # Add extra delay between batches
            if batch_end < len(facilities):
                between_batch_sleep = SLEEP_BETWEEN_REQUESTS * 2
                logger.info(f"Finished batch. Waiting {between_batch_sleep} seconds before starting next batch...")
                time.sleep(between_batch_sleep)
        
        logger.info(f"Finished processing. Fixed {success_count}/{len(facilities)} facilities.")
        
        # Get the final counts
        total = db.session.query(MedicalFacility).count()
        geocoded = db.session.query(MedicalFacility).filter(MedicalFacility.geocoded == True).count()
        with_coords = db.session.query(MedicalFacility).filter(
            MedicalFacility.latitude != None,
            MedicalFacility.longitude != None
        ).count()
        
        logger.info(f"Total facilities: {total}")
        logger.info(f"Geocoded facilities: {geocoded} ({geocoded/total*100:.1f}%)")
        logger.info(f"Facilities with coordinates: {with_coords} ({with_coords/total*100:.1f}%)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix missing coordinates for geocoded facilities")
    parser.add_argument("--max", type=int, help="Maximum number of facilities to process", default=None)
    args = parser.parse_args()
    
    fix_missing_coordinates(max_facilities=args.max)