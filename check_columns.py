import sys
from app import app, db
from models import FacilitySpecialty

with app.app_context():
    columns = [c.name for c in FacilitySpecialty.__table__.columns]
    print("FacilitySpecialty columns:", columns)