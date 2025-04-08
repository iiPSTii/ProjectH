"""Check the original quality scores for specific facilities"""

from app import app, db
from models import MedicalFacility

def check_scores():
    """Check original quality scores for specific facilities"""
    print('Current quality scores:')
    
    facility_names = ['Ospedale SS. Annunziata', 'Ospedale San Pio', 'Policlinico Gemelli']
    
    for name in facility_names:
        facilities = MedicalFacility.query.filter(MedicalFacility.name.like(f'%{name}%')).all()
        for f in facilities:
            print(f'- {f.name}, {f.city}: {f.quality_score}')

if __name__ == "__main__":
    with app.app_context():
        check_scores()