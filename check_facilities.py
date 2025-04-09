"""Check the first few facilities in the database"""
from app import app, db
from models import MedicalFacility

def check_facilities():
    """Print the first few facilities from our database"""
    facilities = MedicalFacility.query.limit(10).all()
    print(f"Found {len(facilities)} facilities")
    for f in facilities:
        region_name = f.region.name if f.region else 'No region'
        print(f"Name: {f.name} | Region: {region_name} | City: {f.city}")

if __name__ == "__main__":
    with app.app_context():
        check_facilities()