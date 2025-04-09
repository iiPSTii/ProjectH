"""
Export Medical Facilities to CSV

This script exports the medical facilities data to a CSV file, 
including the specialty ratings and strengths summary.
"""

import csv
import logging
from app import app, db
from models import MedicalFacility, Region

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def export_facilities_to_csv(filename="updated_medical_facilities.csv"):
    """Export all medical facilities to a CSV file"""
    try:
        logger.info(f"Exporting medical facilities to {filename}")
        
        # Query all facilities with region joined
        facilities = db.session.query(MedicalFacility).join(Region).all()
        
        logger.info(f"Found {len(facilities)} facilities to export")
        
        # Define the CSV columns
        columns = [
            'id', 'name', 'address', 'city', 'postal_code', 'region', 
            'facility_type', 'telephone', 'email', 'website',
            'quality_score',
            'cardiology_rating', 'orthopedics_rating', 'oncology_rating',
            'neurology_rating', 'surgery_rating', 'urology_rating',
            'pediatrics_rating', 'gynecology_rating', 'strengths_summary',
            'data_source', 'attribution'
        ]
        
        # Write to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=columns)
            writer.writeheader()
            
            for facility in facilities:
                row = {
                    'id': facility.id,
                    'name': facility.name,
                    'address': facility.address,
                    'city': facility.city,
                    'postal_code': facility.postal_code,
                    'region': facility.region.name if facility.region else '',
                    'facility_type': facility.facility_type,
                    'telephone': facility.telephone,
                    'email': facility.email,
                    'website': facility.website,
                    'quality_score': facility.quality_score,
                    'cardiology_rating': facility.cardiology_rating,
                    'orthopedics_rating': facility.orthopedics_rating,
                    'oncology_rating': facility.oncology_rating,
                    'neurology_rating': facility.neurology_rating,
                    'surgery_rating': facility.surgery_rating,
                    'urology_rating': facility.urology_rating,
                    'pediatrics_rating': facility.pediatrics_rating,
                    'gynecology_rating': facility.gynecology_rating,
                    'strengths_summary': facility.strengths_summary,
                    'data_source': facility.data_source,
                    'attribution': facility.attribution
                }
                writer.writerow(row)
        
        logger.info(f"Successfully exported {len(facilities)} facilities to {filename}")
        return True
    
    except Exception as e:
        logger.error(f"Error exporting facilities to CSV: {e}")
        return False

def main():
    """Main function to export facilities to CSV"""
    logger.info("Starting export to CSV")
    
    with app.app_context():
        # Export facilities to CSV
        if export_facilities_to_csv():
            print("Updated dataset saved successfully.")
        else:
            print("Error saving updated dataset.")

if __name__ == "__main__":
    main()