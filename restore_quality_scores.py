"""
Restore Original Quality Scores

This script restores the original quality scores for facilities that had their
quality scores updated based on specialty ratings.
"""

import logging
from app import app, db
from models import MedicalFacility, DatabaseStatus

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Manually define the original quality scores for the affected facilities
ORIGINAL_SCORES = {
    'Ospedale SS. Annunziata': 3.6,
    'Ospedale San Pio': 3.2,
    'Policlinico Gemelli': 4.7
}

def restore_quality_scores():
    """Restore original quality scores for facilities"""
    stats = {
        'total_restored': 0,
        'total_attempted': len(ORIGINAL_SCORES),
        'errors': 0
    }
    
    for facility_name, original_score in ORIGINAL_SCORES.items():
        try:
            # Find all facilities with this name
            facilities = MedicalFacility.query.filter(
                MedicalFacility.name.ilike(f"%{facility_name}%")
            ).all()
            
            if facilities:
                logger.info(f"Found {len(facilities)} matches for {facility_name}")
                for facility in facilities:
                    # Log current score
                    logger.info(f"Restoring {facility.name} (ID: {facility.id}) quality score:")
                    logger.info(f"  Current score: {facility.quality_score}")
                    
                    # Update score
                    facility.quality_score = original_score
                    
                    # Log new score
                    logger.info(f"  Restored score: {facility.quality_score}")
                    
                    stats['total_restored'] += 1
            else:
                logger.warning(f"No match found for {facility_name}")
        
        except Exception as e:
            logger.error(f"Error restoring score for {facility_name}: {e}")
            stats['errors'] += 1
    
    # Commit all changes
    try:
        db.session.commit()
        logger.info(f"Successfully restored {stats['total_restored']} facility quality scores")
    except Exception as e:
        logger.error(f"Error committing changes: {e}")
        db.session.rollback()
        stats['errors'] += stats['total_restored']
        stats['total_restored'] = 0
    
    return stats

def update_database_status(stats):
    """Update the database status to reflect the score restoration"""
    try:
        current_status = DatabaseStatus.get_status()
        
        if current_status:
            status_notes = (
                f"Restored original quality scores for {stats['total_restored']} facilities. "
                f"({stats['errors']} errors)"
            )
            
            # Create new status record
            DatabaseStatus.update_status(
                status="initialized",
                total_facilities=current_status.total_facilities,
                total_regions=current_status.total_regions,
                total_specialties=current_status.total_specialties,
                notes=status_notes,
                initialized_by="restore_quality_scores.py"
            )
            
            logger.info("Database status updated successfully")
            return True
        else:
            logger.warning("No current database status found, skipping status update")
            return False
    except Exception as e:
        logger.error(f"Error updating database status: {e}")
        return False

def main():
    """Main function to restore original quality scores"""
    logger.info("Starting quality score restoration")
    
    with app.app_context():
        # Restore quality scores
        stats = restore_quality_scores()
        
        # Update database status
        update_database_status(stats)
        
        logger.info("Quality score restoration completed successfully")
        print("Original quality scores restored successfully.")
        
        return True

if __name__ == "__main__":
    main()