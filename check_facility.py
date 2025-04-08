"""Check facility data for a specific facility"""

from app import app, db
from models import MedicalFacility

def check_facility():
    """Check facility data for 'Ospedale SS. Annunziata'"""
    try:
        facility = MedicalFacility.query.filter(MedicalFacility.name == 'Ospedale SS. Annunziata').first()
        
        if facility:
            print(f'Facility: {facility.name}')
            print(f'Cardiology Rating: {facility.cardiology_rating}')
            print(f'Oncology Rating: {facility.oncology_rating}')
            print(f'Strengths Summary: {facility.strengths_summary}')
        else:
            print("Facility not found")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    with app.app_context():
        check_facility()