"""
Test Import Script

This script creates a small CSV file with sample data and runs the import process
to test the update_specialty_ratings.py script.
"""

import os
import csv
import logging
from update_specialty_ratings import update_specialty_ratings
from sqlalchemy.orm import Session
from app import db
from models import MedicalFacility, Specialty

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_csv():
    """Create a small CSV file with sample data for testing"""
    test_file = "test_ratings.csv"
    
    # Get some real facility and specialty IDs from the database
    with Session(db.engine) as session:
        # Get a few facilities
        facilities = session.query(MedicalFacility).limit(5).all()
        if not facilities:
            logger.error("No facilities found in the database")
            return None
        
        # Get a few specialties
        specialties = session.query(Specialty).limit(3).all()
        if not specialties:
            logger.error("No specialties found in the database")
            return None
        
        # Create test data
        test_data = []
        for facility in facilities:
            for specialty in specialties:
                # Create a test entry with a new rating
                test_data.append({
                    'facility_id': facility.id,
                    'specialty_id': specialty.id,
                    'specialty_name': specialty.name,
                    'quality_rating': round(min(facility.quality_score + 0.5, 5.0), 1)  # Slightly higher rating
                })
        
        # Write the test data to a CSV file
        with open(test_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['facility_id', 'specialty_id', 'specialty_name', 'quality_rating']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in test_data:
                writer.writerow(row)
        
        logger.info(f"Created test CSV file with {len(test_data)} rows: {test_file}")
        return test_file

if __name__ == "__main__":
    # Create a test CSV file
    test_file = create_test_csv()
    if not test_file:
        print("Failed to create test CSV file")
        exit(1)
    
    try:
        # Run the update process
        print(f"Running test import from {test_file}...")
        stats = update_specialty_ratings(test_file)
        
        print("\nTest import completed:")
        print(f"  Processed: {stats['processed']} rows")
        print(f"  Updated:   {stats['updated']} ratings")
        print(f"  Created:   {stats['created']} new ratings")
        print(f"  Unchanged: {stats['unchanged']} ratings")
        print(f"  Errors:    {stats['errors']} rows")
    
    finally:
        # Clean up the test file
        if os.path.exists(test_file):
            os.remove(test_file)
            print(f"Cleaned up test file: {test_file}")