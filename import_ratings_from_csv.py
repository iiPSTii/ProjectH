"""
Import Ratings from CSV

This script imports specialty ratings from a user-provided CSV file.
It validates the file format and calls the update_specialty_ratings.py script
to update the database.

Usage:
  python import_ratings_from_csv.py path/to/ratings.csv
"""

import sys
import os
import csv
import logging
from update_specialty_ratings import update_specialty_ratings
from app import app

# Setup logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_csv(file_path):
    """
    Validate the CSV file format
    
    Args:
        file_path: Path to the CSV file
    
    Returns:
        bool: True if the file is valid, False otherwise
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return False
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Check if it's a valid CSV
            reader = csv.reader(f)
            header = next(reader)
            
            # Check required fields
            required_fields = ["facility_id", "quality_rating"]
            if not all(field in header for field in required_fields):
                missing = [f for f in required_fields if f not in header]
                logger.error(f"CSV is missing required fields: {', '.join(missing)}")
                return False
            
            # Check if specialty identification is present
            if "specialty_id" not in header and "specialty_name" not in header:
                logger.error("CSV must contain either 'specialty_id' or 'specialty_name' column")
                return False
            
            # Read a few rows to check data integrity
            row_count = 0
            errors = 0
            for row in reader:
                row_count += 1
                if len(row) != len(header):
                    logger.warning(f"Row {row_count} has {len(row)} columns, expected {len(header)}")
                    errors += 1
                
                if row_count >= 10:  # Check first 10 rows
                    break
            
            if errors > 0:
                logger.warning(f"Found {errors} errors in the first {row_count} rows")
                return False
            
            return True
    
    except Exception as e:
        logger.error(f"Error validating CSV file: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python import_ratings_from_csv.py path/to/ratings.csv")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    print(f"Validating CSV file: {csv_file}...")
    
    if not validate_csv(csv_file):
        print("CSV validation failed. Please check the file format and try again.")
        sys.exit(1)
    
    print(f"CSV file validated. Importing ratings from {csv_file}...")
    
    # Use the Flask application context
    with app.app_context():
        try:
            stats = update_specialty_ratings(csv_file)
            
            print("\nImport completed successfully:")
            print(f"  Processed: {stats['processed']} rows")
            print(f"  Updated:   {stats['updated']} ratings")
            print(f"  Created:   {stats['created']} new ratings")
            print(f"  Unchanged: {stats['unchanged']} ratings")
            print(f"  Errors:    {stats['errors']} rows")
            
            print("\nDatabase has been updated with new specialty ratings.")
            print("A backup of the previous database state has been created.")
        
        except Exception as e:
            print(f"Error importing ratings: {e}")
            sys.exit(1)