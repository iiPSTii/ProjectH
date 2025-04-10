"""
Update Database Status

This script updates the database status to reflect the manual addition of regions.
"""

from app import app, db
from models import DatabaseStatus, Region, Specialty, MedicalFacility
import datetime

def update_status():
    """Update the database status with current counts"""
    with app.app_context():
        # Get current counts
        region_count = Region.query.count()
        specialty_count = Specialty.query.count()
        facility_count = MedicalFacility.query.count()
        
        # Create new status
        new_status = DatabaseStatus(
            status="updated",
            last_updated=datetime.datetime.utcnow(),
            total_facilities=facility_count,
            total_regions=region_count,
            total_specialties=specialty_count,
            notes="Manually updated regions to include all 20 Italian regions",
            initialized_by="update_db_status.py"
        )
        
        db.session.add(new_status)
        db.session.commit()
        
        print(f"Updated database status:")
        print(f"- Total facilities: {facility_count}")
        print(f"- Total regions: {region_count}")
        print(f"- Total specialties: {specialty_count}")
        print(f"- Last updated: {new_status.last_updated}")

if __name__ == "__main__":
    update_status()