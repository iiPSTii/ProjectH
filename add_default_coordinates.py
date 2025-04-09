"""
Add Default Coordinates for Facilities

This script adds default coordinates based on the facility's region for
facilities that are marked as geocoded but are missing latitude and longitude.
"""

import logging
import sys

from app import app, db
from models import MedicalFacility, Region

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

# Default coordinates for Italian regions
REGION_DEFAULT_COORDINATES = {
    'Abruzzo': (42.35, 13.40),
    'Basilicata': (40.64, 15.80), 
    'Calabria': (38.90, 16.59),
    'Campania': (40.83, 14.25),
    'Emilia-Romagna': (44.49, 11.34),
    'Friuli Venezia Giulia': (45.64, 13.78),
    'Friuli-Venezia Giulia': (45.64, 13.78),  # Alternative spelling
    'Lazio': (41.90, 12.49),
    'Liguria': (44.41, 8.93),
    'Lombardia': (45.46, 9.19),
    'Marche': (43.61, 13.51),
    'Molise': (41.56, 14.66),
    'Piemonte': (45.07, 7.69),
    'Puglia': (41.12, 16.86),
    'Sardegna': (39.22, 9.10),
    'Sicilia': (38.11, 13.36),
    'Toscana': (43.77, 11.25),
    'Trentino-Alto Adige': (46.07, 11.12),
    'Trentino Alto Adige': (46.07, 11.12),  # Alternative spelling
    'Umbria': (43.11, 12.39),
    'Valle d\'Aosta': (45.74, 7.32),
    'Valle d\'Aosta/Vallée d\'Aoste': (45.74, 7.32),  # Alternative spelling
    'Veneto': (45.44, 12.32)
}

# A default fallback for unknown regions
DEFAULT_COORDINATES = (41.90, 12.49)  # Rome/center of Italy

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

def get_region_coordinates(region_name):
    """Get default coordinates for a region"""
    if not region_name:
        return DEFAULT_COORDINATES
    
    return REGION_DEFAULT_COORDINATES.get(region_name, DEFAULT_COORDINATES)

def add_default_coordinates():
    """Add default coordinates for facilities missing them"""
    with app.app_context():
        # Get statistics before processing
        logger.info("Getting statistics before processing...")
        stats_before = get_stats()
        
        if stats_before['missing'] == 0:
            logger.info("No facilities with missing coordinates found!")
            return 0
        
        # Get all facilities missing coordinates
        facilities = db.session.query(MedicalFacility).filter(
            MedicalFacility.geocoded == True,
            MedicalFacility.latitude.is_(None)
        ).all()
        
        logger.info(f"Processing {len(facilities)} facilities with missing coordinates")
        
        # Create a mapping of region IDs to region names
        regions = db.session.query(Region).all()
        region_map = {region.id: region.name for region in regions}
        
        updated_count = 0
        
        for facility in facilities:
            region_name = region_map.get(facility.region_id, None)
            
            logger.info(f"Processing facility {facility.id}: {facility.name} (Region: {region_name})")
            
            # Get default coordinates for this region
            lat, lon = get_region_coordinates(region_name)
            
            # Add a small random offset (±0.05 degrees) to avoid all facilities
            # in the same region appearing at exactly the same point
            import random
            lat_offset = random.uniform(-0.05, 0.05)
            lon_offset = random.uniform(-0.05, 0.05)
            
            facility.latitude = lat + lat_offset
            facility.longitude = lon + lon_offset
            
            updated_count += 1
        
        # Commit all changes at once
        try:
            db.session.commit()
            logger.info(f"Successfully updated {updated_count} facilities")
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error updating facilities: {str(e)}")
            return 1
        
        # Get statistics after processing
        logger.info("Getting statistics after processing...")
        get_stats()
        
        return 0

if __name__ == "__main__":
    sys.exit(add_default_coordinates())