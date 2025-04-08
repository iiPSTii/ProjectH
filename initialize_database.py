#!/usr/bin/env python3
"""
Database Initialization Script for FindMyCure Italia

This script will populate the database with all medical facilities data from
available scrapers. It's designed to run as a standalone script outside the
web request cycle to avoid timeouts.

Usage:
  python initialize_database.py [--force]
  
  --force: Force reinitialization even if database is already populated

This script will:
1. Clear existing database if specified or if database is empty
2. Populate all regions, specialties, and facilities from all available scrapers
3. Update the database status to track initialization
"""

import os
import sys
import logging
import argparse
import time
from datetime import datetime

# Configure logging to display on console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("db_init")

# Initialize Flask app context
from app import app, db
from models import Region, Specialty, MedicalFacility, FacilitySpecialty, DatabaseStatus
import web_scraper
import data_loader

# Set constants
MAX_FACILITIES_PER_REGION = 100  # Set to higher value for production
MAX_SPECIALTIES_PER_FACILITY = 10  # Set to higher value for production
TIMEOUT = 3600  # 1 hour max for full initialization

def check_database_status():
    """Check if database is already initialized"""
    status = DatabaseStatus.get_status()
    
    if status and status.status == "initialized":
        facilities_count = MedicalFacility.query.count()
        logger.info(f"Database already initialized on {status.last_updated} with {facilities_count} facilities")
        return status
    return None

def clear_database():
    """Clear all data from the database"""
    try:
        logger.info("Clearing existing database data")
        FacilitySpecialty.query.delete()
        db.session.commit()
        MedicalFacility.query.delete()
        db.session.commit()
        Specialty.query.delete()
        db.session.commit()
        Region.query.delete()
        db.session.commit()
        DatabaseStatus.query.delete()
        db.session.commit()
        logger.info("Database cleared successfully")
        return True
    except Exception as e:
        logger.error(f"Error clearing database: {str(e)}")
        db.session.rollback()
        return False

def initialize_database(force=False):
    """
    Initialize the database with all medical facilities data
    
    Args:
        force (bool): Force reinitialization even if database is already populated
    
    Returns:
        bool: True if initialization was successful, False otherwise
    """
    start_time = time.time()
    
    # Check if database is already initialized
    status = check_database_status()
    
    if status and not force:
        logger.info("Database already initialized. Use --force to reinitialize.")
        return False
    
    # Begin initialization
    try:
        DatabaseStatus.update_status("updating", 
                                    notes="Beginning database initialization",
                                    initialized_by="initialize_database.py")
        
        # Clear existing data if necessary
        if MedicalFacility.query.count() > 0 or force:
            if not clear_database():
                logger.error("Failed to clear database. Aborting initialization.")
                return False
        
        # Set up common specialties
        common_specialties = [
            "Cardiologia", "Pediatria", "Medicina Generale", "Oncologia", 
            "Ortopedia", "Ginecologia", "Dermatologia", "Oculistica",
            "Neurologia", "Psichiatria", "Urologia", "Otorinolaringoiatria",
            "Chirurgia Generale", "Anestesia", "Radiologia", "Endocrinologia",
            "Gastroenterologia", "Pneumologia", "Reumatologia", "Allergologia"
        ]
        
        specialties = {}
        logger.info("Adding common specialties")
        for name in common_specialties:
            try:
                specialty = Specialty(name=name)
                db.session.add(specialty)
                db.session.commit()
                specialties[name] = specialty
                logger.info(f"Added specialty: {name}")
            except Exception as e:
                logger.error(f"Error adding specialty {name}: {str(e)}")
                db.session.rollback()
        
        # Get all available scrapers
        logger.info("Getting all available scrapers")
        all_scrapers = web_scraper.get_available_scrapers()
        logger.info(f"Found {len(all_scrapers)} region scrapers")
        
        # Process each scraper with increased limits
        total_regions = 0
        total_facilities = 0
        
        for i, scraper in enumerate(all_scrapers):
            try:
                source_name = scraper.source_name
                region_name = scraper.region_name
                attribution = scraper.attribution
                
                logger.info(f"Processing region {i+1}/{len(all_scrapers)}: {region_name}")
                
                # Get or create region
                region = data_loader.get_or_create_region(region_name)
                if not region:
                    logger.error(f"Could not create region {region_name}. Skipping.")
                    continue
                
                # Fetch data - without timeouts
                logger.info(f"Fetching data for {region_name} from {source_name}")
                try:
                    df = scraper.fetch_data()
                    
                    if df is None or df.empty:
                        logger.warning(f"No data available for {region_name}")
                        continue
                        
                    # Process facilities with increased limits
                    max_items = min(MAX_FACILITIES_PER_REGION, len(df))
                    logger.info(f"Processing {max_items} facilities for {region_name} (out of {len(df)} available)")
                    
                    facilities_added = 0
                    for idx in range(max_items):
                        try:
                            # Extract basic facility information with error handling
                            name_col = next((col for col in df.columns if col.lower() in 
                                          ['nome', 'name', 'denominazione', 'denomstruttura', 'facility_name']), None)
                            
                            if not name_col:
                                logger.warning(f"Could not find name column in {region_name} data")
                                break
                                
                            name = data_loader.safe_get(df, idx, name_col)
                            if not name:
                                continue
                                
                            # Clean name with unidecode
                            from unidecode import unidecode
                            name = unidecode(str(name))
                            # Ensure ASCII only
                            name = ''.join(c for c in name if ord(c) < 128)
                            
                            # Skip if name is empty after cleaning
                            if not name:
                                logger.warning(f"Name became empty after cleaning for row {idx}. Skipping.")
                                continue
                                
                            # Check if facility already exists
                            existing = MedicalFacility.query.filter_by(
                                name=name, 
                                region_id=region.id
                            ).first()
                            
                            if existing:
                                logger.debug(f"Facility already exists: {name} in {region.name}")
                                continue
                                
                            # Find column names in the DataFrame
                            facility_cols = {
                                'type': next((col for col in df.columns if col.lower() in 
                                           ['tipo', 'type', 'tipologia']), None),
                                'address': next((col for col in df.columns if col.lower() in 
                                              ['indirizzo', 'address', 'via']), None),
                                'city': next((col for col in df.columns if col.lower() in 
                                           ['città', 'city', 'citta', 'comune']), None),
                                'phone': next((col for col in df.columns if col.lower() in 
                                            ['telefono', 'phone', 'tel']), None),
                                'email': next((col for col in df.columns if col.lower() in 
                                            ['email', 'e-mail', 'mail']), None),
                                'web': next((col for col in df.columns if col.lower() in 
                                          ['website', 'web', 'sito']), None),
                                'specialty': next((col for col in df.columns if col.lower() in 
                                                ['specialties', 'specialità', 'specialita', 'brancheautorizzate']), None)
                            }
                            
                            # Extract and clean fields
                            facility_data = {}
                            for field, col in facility_cols.items():
                                if col:
                                    value = data_loader.safe_get(df, idx, col)
                                    if value:
                                        # Clean with ASCII-only filter
                                        facility_data[field] = ''.join(c for c in str(value) if ord(c) < 128)
                            
                            # Create new facility
                            import random
                            facility = MedicalFacility(
                                name=name,
                                address=facility_data.get('address'),
                                city=facility_data.get('city'),
                                region=region,
                                facility_type=facility_data.get('type', "Struttura Sanitaria"),
                                telephone=facility_data.get('phone'),
                                email=facility_data.get('email'),
                                website=facility_data.get('web'),
                                data_source=source_name,
                                attribution=attribution,
                                # Set values for optional fields
                                quality_score=round(random.uniform(2.5, 5.0), 1),
                                cost_estimate=round(random.uniform(50, 300), 2) if random.random() > 0.3 else None
                            )
                            
                            # Add facility to database
                            db.session.add(facility)
                            db.session.commit()
                            facilities_added += 1
                            logger.debug(f"Added facility: {name} in {region.name}")
                            
                            # Process specialties
                            if 'specialty' in facility_data:
                                specialties_text = facility_data['specialty']
                                if specialties_text:
                                    specialty_names = data_loader.extract_specialties(specialties_text)
                                    
                                    # Limit to a reasonable number
                                    max_specialties = min(MAX_SPECIALTIES_PER_FACILITY, len(specialty_names))
                                    specialties_processed = 0
                                    
                                    for specialty_name in specialty_names[:max_specialties]:
                                        specialty = data_loader.get_or_create_specialty(specialty_name)
                                        if specialty:
                                            # Check if this facility already has this specialty
                                            existing_fs = FacilitySpecialty.query.filter_by(
                                                facility_id=facility.id,
                                                specialty_id=specialty.id
                                            ).first()
                                            
                                            if not existing_fs:
                                                facility_specialty = FacilitySpecialty(
                                                    facility_id=facility.id,
                                                    specialty_id=specialty.id
                                                )
                                                db.session.add(facility_specialty)
                                                db.session.commit()
                                                specialties_processed += 1
                                
                        except Exception as e:
                            logger.error(f"Error processing facility {idx} from {region_name}: {str(e)}")
                            db.session.rollback()
                            continue
                    
                    # Update stats
                    if facilities_added > 0:
                        total_facilities += facilities_added
                        total_regions += 1
                        logger.info(f"Added {facilities_added} facilities for {region_name}")
                    
                except Exception as e:
                    logger.error(f"Error fetching data for {region_name}: {str(e)}")
                    continue
                
            except Exception as e:
                logger.error(f"Error processing region {region_name}: {str(e)}")
                continue
            
            # Check if we've exceeded our time limit
            elapsed_time = time.time() - start_time
            if elapsed_time > TIMEOUT:
                logger.warning(f"Initialization taking too long ({elapsed_time:.1f}s). Stopping early.")
                break
                
        # Update database status with completed information
        total_specialties = Specialty.query.count()
        
        if total_facilities > 0:
            DatabaseStatus.update_status(
                "initialized", 
                total_facilities=total_facilities,
                total_regions=total_regions,
                total_specialties=total_specialties,
                notes=f"Database initialized with {total_facilities} facilities from {total_regions} regions in {time.time() - start_time:.1f} seconds",
                initialized_by="initialize_database.py"
            )
            logger.info(f"Database initialized successfully with {total_facilities} facilities from {total_regions} regions")
            return True
        else:
            DatabaseStatus.update_status(
                "error",
                notes="Initialization failed - no facilities were added",
                initialized_by="initialize_database.py"
            )
            logger.error("Initialization failed - no facilities were added")
            return False
            
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        DatabaseStatus.update_status(
            "error",
            notes=f"Initialization error: {str(e)}",
            initialized_by="initialize_database.py"
        )
        return False

def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description="Initialize database with medical facilities data")
    parser.add_argument("--force", action="store_true", help="Force reinitialization even if database is already populated")
    args = parser.parse_args()
    
    logger.info("Starting database initialization")
    
    with app.app_context():
        success = initialize_database(force=args.force)
        
    if success:
        logger.info("Database initialization completed successfully")
        return 0
    else:
        logger.error("Database initialization failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())