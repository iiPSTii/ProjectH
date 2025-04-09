"""
Generate Specialty Ratings for Italian Medical Facilities

This script will generate realistic specialty ratings for facilities in the database
and save them to a JSON file that can be used by update_facility_ratings.py.
"""

import json
import random
import logging
from app import app, db
from models import MedicalFacility, FacilitySpecialty, Specialty

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Output file
OUTPUT_FILE = 'attached_assets/italian_medical_facilities_ratings.json'

# Specialty strengths descriptions
STRENGTH_TEMPLATES = [
    "Outstanding in {specialty}.",
    "Excels in {specialty} and {specialty2}.",
    "Known for excellence in {specialty}.",
    "Leading center for {specialty} in {region}.",
    "Top-rated for {specialty} treatments.",
    "Regional reference for {specialty} and {specialty2}.",
    "Renowned for {specialty} expertise."
]

def generate_specialty_ratings():
    """Generate specialty ratings for facilities in the database"""
    try:
        # Get all facilities with region information
        facilities = MedicalFacility.query.filter(MedicalFacility.region_id != None).all()
        logger.info(f"Found {len(facilities)} facilities with region information")

        # Get all specialties
        all_specialties = Specialty.query.all()
        specialty_names = [s.name for s in all_specialties]
        
        # List to store facility rating data
        ratings_data = []
        
        for facility in facilities:
            # Get the facility's specialties
            facility_specialties = [fs.specialty.name for fs in facility.specialties]
            
            # Generate ratings only if the facility has specialties
            if facility_specialties:
                # Create ratings dictionary for this facility
                facility_rating = {
                    "facility": facility.name,
                    "city": facility.city or "",
                    "region": facility.region.name if facility.region else "",
                }
                
                # Generate specialty ratings
                cardiology_rating = random.randint(3, 5) if "Cardiologia" in facility_specialties else random.randint(1, 3)
                orthopedics_rating = random.randint(3, 5) if "Ortopedia" in facility_specialties else random.randint(1, 3)
                oncology_rating = random.randint(3, 5) if "Oncologia" in facility_specialties else random.randint(1, 3)
                neurology_rating = random.randint(3, 5) if "Neurologia" in facility_specialties else random.randint(1, 3)
                surgery_rating = random.randint(3, 5) if "Chirurgia" in facility_specialties else random.randint(1, 3)
                urology_rating = random.randint(3, 5) if "Urologia" in facility_specialties else random.randint(1, 3)
                pediatrics_rating = random.randint(3, 5) if "Pediatria" in facility_specialties else random.randint(1, 3)
                gynecology_rating = random.randint(3, 5) if "Ginecologia" in facility_specialties else random.randint(1, 3)
                
                # Add ratings to dictionary
                facility_rating["cardiology_rating"] = cardiology_rating
                facility_rating["orthopedics_rating"] = orthopedics_rating
                facility_rating["oncology_rating"] = oncology_rating
                facility_rating["neurology_rating"] = neurology_rating
                facility_rating["surgery_rating"] = surgery_rating
                facility_rating["urology_rating"] = urology_rating
                facility_rating["pediatrics_rating"] = pediatrics_rating
                facility_rating["gynecology_rating"] = gynecology_rating
                
                # Get the highest rated specialties
                ratings = {
                    "Cardiologia": cardiology_rating,
                    "Ortopedia": orthopedics_rating,
                    "Oncologia": oncology_rating,
                    "Neurologia": neurology_rating,
                    "Chirurgia": surgery_rating,
                    "Urologia": urology_rating,
                    "Pediatria": pediatrics_rating,
                    "Ginecologia": gynecology_rating
                }
                
                # Sort specialties by rating (highest first)
                top_specialties = sorted(
                    [(name, rating) for name, rating in ratings.items() if rating >= 4],
                    key=lambda x: x[1], 
                    reverse=True
                )
                
                # Generate strengths summary
                if top_specialties:
                    if len(top_specialties) >= 2:
                        template = random.choice(STRENGTH_TEMPLATES)
                        specialty1 = top_specialties[0][0]
                        specialty2 = top_specialties[1][0]
                        
                        # Replace placeholders in template
                        summary = template.format(
                            specialty=specialty1,
                            specialty2=specialty2,
                            region=facility.region.name if facility.region else "Italia"
                        )
                    else:
                        # Only one top specialty
                        specialty = top_specialties[0][0]
                        summary = f"Outstanding in {specialty}."
                else:
                    # No top specialties
                    summary = ""
                
                facility_rating["strengths_summary"] = summary
                
                # Add to ratings data list
                ratings_data.append(facility_rating)
                logger.debug(f"Generated ratings for {facility.name}")
        
        # Save to JSON file
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(ratings_data, f, indent=4, ensure_ascii=False)
        
        logger.info(f"Successfully generated ratings for {len(ratings_data)} facilities")
        return len(ratings_data)
    
    except Exception as e:
        logger.error(f"Error generating specialty ratings: {e}")
        return 0

def main():
    """Main function"""
    logger.info("Starting specialty ratings generation")
    
    with app.app_context():
        # Generate ratings
        count = generate_specialty_ratings()
        
        if count > 0:
            logger.info(f"Generated specialty ratings for {count} facilities")
            print(f"Generated and saved specialty ratings for {count} facilities to {OUTPUT_FILE}")
        else:
            logger.error("Failed to generate specialty ratings")
            print("Failed to generate specialty ratings")

if __name__ == "__main__":
    main()