"""
Restore Medical Facilities from JSON file

This script restores all medical facilities from the JSON backup file.
It creates all facilities, regions, and specialties that were in the JSON file.
"""

import json
import logging
import sys
from sqlalchemy.exc import SQLAlchemyError

from app import app, db
from models import MedicalFacility, Region, Specialty, FacilitySpecialty, DatabaseStatus

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JSON_FILE_PATH = "attached_assets/medical_facilities_from_txt.json"

def load_json_data():
    """Load the facility data from the provided JSON file"""
    try:
        with open(JSON_FILE_PATH, 'r') as json_file:
            return json.load(json_file)
    except Exception as e:
        logger.error(f"Error loading JSON file: {str(e)}")
        return None

def get_or_create_region(region_name):
    """Get or create a region by name"""
    region = Region.query.filter(Region.name == region_name).first()
    if not region and region_name:
        region = Region(name=region_name)
        db.session.add(region)
        db.session.commit()
        logger.info(f"Created new region: {region_name}")
    return region

def restore_facilities(json_data):
    """
    Restore facilities from JSON data
    
    Args:
        json_data: List of dictionaries with facility data
    
    Returns:
        dict: Statistics about the restoration
    """
    stats = {
        'total_processed': 0,
        'created': 0,
        'updated': 0,
        'errors': 0
    }
    
    # First ensure all regions exist
    regions = [
        "Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna", 
        "Friuli-Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche", 
        "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia", "Toscana", 
        "Trentino", "Umbria", "Valle d'Aosta", "Veneto"
    ]
    
    for region_name in regions:
        get_or_create_region(region_name)
    
    try:
        # Clear existing facilities to avoid duplicates
        db.session.query(FacilitySpecialty).delete()
        db.session.query(MedicalFacility).delete()
        db.session.commit()
        logger.info("Cleared existing facilities")
        
        for facility_data in json_data:
            stats['total_processed'] += 1
            
            # Skip entries without facility name
            if not facility_data.get("Facility") or facility_data["Facility"] == "__":
                continue
            
            facility_name = facility_data["Facility"]
            city = facility_data.get("City", "")
            region_name = facility_data.get("Region", "Puglia")  # Default to Puglia if no region
            
            try:
                # Get or create the region
                region = get_or_create_region(region_name)
                
                # Create the facility
                facility = MedicalFacility(
                    name=facility_name,
                    city=city,
                    region_id=region.id if region else None,
                    facility_type="Ospedale",
                    address=facility_data.get("Address", ""),
                    postal_code=facility_data.get("PostalCode", ""),
                    telephone=facility_data.get("Phone", ""),
                    email=facility_data.get("Email", ""),
                    website=facility_data.get("Website", ""),
                    quality_score=4.0,  # Default quality score
                    data_source="Restored from JSON",
                    attribution="FindMyCure Italia Backup"
                )
                
                # Set specialty ratings
                if facility_data.get("Cardio") and facility_data["Cardio"].isdigit():
                    facility.cardiology_rating = float(facility_data["Cardio"])
                
                if facility_data.get("Ortho") and facility_data["Ortho"].isdigit():
                    facility.orthopedics_rating = float(facility_data["Ortho"])
                
                if facility_data.get("Onco") and facility_data["Onco"].isdigit():
                    facility.oncology_rating = float(facility_data["Onco"])
                
                if facility_data.get("Neuro") and facility_data["Neuro"].isdigit():
                    facility.neurology_rating = float(facility_data["Neuro"])
                
                if facility_data.get("Surg") and facility_data["Surg"].isdigit():
                    facility.surgery_rating = float(facility_data["Surg"])
                
                if facility_data.get("Uro") and facility_data["Uro"].isdigit():
                    facility.urology_rating = float(facility_data["Uro"])
                
                if facility_data.get("Ped") and facility_data["Ped"].isdigit():
                    facility.pediatrics_rating = float(facility_data["Ped"])
                
                if facility_data.get("Gyn") and facility_data["Gyn"].isdigit():
                    facility.gynecology_rating = float(facility_data["Gyn"])
                
                # Set geolocation data
                if facility_data.get("latitude") is not None:
                    facility.latitude = float(facility_data["latitude"])
                
                if facility_data.get("longitude") is not None:
                    facility.longitude = float(facility_data["longitude"])
                
                if facility_data.get("geocoded") is not None:
                    facility.geocoded = facility_data["geocoded"]
                
                # Set strengths summary
                if facility_data.get("Strengths"):
                    facility.strengths_summary = facility_data["Strengths"]
                    
                # Add facility to database
                db.session.add(facility)
                stats['created'] += 1
                
                # Commit every 50 facilities to avoid large transactions
                if stats['created'] % 50 == 0:
                    db.session.commit()
                    logger.info(f"Created {stats['created']} facilities so far")
                
            except Exception as e:
                logger.error(f"Error creating facility {facility_name}: {str(e)}")
                stats['errors'] += 1
                continue
        
        # Commit all pending changes
        db.session.commit()
        logger.info(f"Successfully created {stats['created']} facilities")
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error restoring facilities: {str(e)}")
        stats['errors'] += 1
    
    return stats

def update_database_status(stats):
    """Update the database status to reflect the restoration"""
    try:
        notes = f"Restored {stats['created']} facilities from JSON backup. {stats['errors']} errors."
        
        DatabaseStatus.update_status(
            status="restored",
            total_facilities=stats['created'],
            total_regions=20,  # We ensure all 20 regions exist
            total_specialties=22,  # Approximate number of specialties
            notes=notes,
            initialized_by="restore_from_json.py"
        )
        logger.info("Database status updated successfully")
    except Exception as e:
        logger.error(f"Error updating database status: {str(e)}")

def main():
    """Main function to restore facilities from JSON file"""
    with app.app_context():
        logger.info("Starting restoration of facilities from JSON file")
        
        # Load JSON data
        json_data = load_json_data()
        if not json_data:
            logger.error("Failed to load JSON data. Exiting.")
            sys.exit(1)
        
        logger.info(f"Loaded {len(json_data)} facilities from JSON file")
        
        # Restore facilities
        stats = restore_facilities(json_data)
        
        # Update database status
        update_database_status(stats)
        
        # Print summary
        logger.info("=== Restoration Summary ===")
        logger.info(f"Total processed: {stats['total_processed']}")
        logger.info(f"Created: {stats['created']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info("==========================")

if __name__ == "__main__":
    main()