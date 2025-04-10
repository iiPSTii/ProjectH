"""
Add missing regions to the database

This script adds all 20 Italian regions to the database if they don't already exist.
"""

from app import app, db
from models import Region
import logging

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('add_regions')

def add_regions():
    """Add all 20 Italian regions to the database"""
    italian_regions = [
        "Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna", 
        "Friuli-Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche", 
        "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia", "Toscana", 
        "Trentino", "Umbria", "Valle d'Aosta", "Veneto"
    ]
    
    added_count = 0
    
    with app.app_context():
        existing_regions = {r.name for r in Region.query.all()}
        logger.info(f"Found {len(existing_regions)} existing regions: {', '.join(existing_regions)}")
        
        for region_name in italian_regions:
            if region_name not in existing_regions:
                region = Region(name=region_name)
                db.session.add(region)
                added_count += 1
                logger.info(f"Added region: {region_name}")
        
        if added_count > 0:
            db.session.commit()
            logger.info(f"Added {added_count} missing regions to the database")
        else:
            logger.info("No new regions added")
        
        # Check total regions again
        total_regions = Region.query.count()
        logger.info(f"Total regions in database: {total_regions}")
        
        return {
            "added": added_count,
            "total": total_regions
        }

if __name__ == "__main__":
    add_regions()