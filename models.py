from app import db
from sqlalchemy import Column, Integer, String, Float, ForeignKey, Table
from sqlalchemy.orm import relationship

class Region(db.Model):
    __tablename__ = 'regions'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    
    facilities = relationship("MedicalFacility", back_populates="region")
    
    def __repr__(self):
        return f"<Region {self.name}>"

class Specialty(db.Model):
    __tablename__ = 'specialties'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    
    facilities = relationship("FacilitySpecialty", back_populates="specialty")
    
    def __repr__(self):
        return f"<Specialty {self.name}>"

class FacilitySpecialty(db.Model):
    __tablename__ = 'facility_specialty'
    
    facility_id = db.Column(db.Integer, ForeignKey('medical_facilities.id'), primary_key=True)
    specialty_id = db.Column(db.Integer, ForeignKey('specialties.id'), primary_key=True)
    
    facility = relationship("MedicalFacility", back_populates="specialties")
    specialty = relationship("Specialty", back_populates="facilities")

class MedicalFacility(db.Model):
    __tablename__ = 'medical_facilities'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    address = db.Column(db.String(200))
    city = db.Column(db.String(100))
    postal_code = db.Column(db.String(20))
    region_id = db.Column(db.Integer, ForeignKey('regions.id'))
    facility_type = db.Column(db.String(100))
    
    # Optional fields
    telephone = db.Column(db.String(50))
    email = db.Column(db.String(100))
    website = db.Column(db.String(200))
    
    # Performance metrics
    cost_estimate = db.Column(db.Float, default=None)
    quality_score = db.Column(db.Float, default=None)
    
    # Data source and attribution
    data_source = db.Column(db.String(100))
    attribution = db.Column(db.String(200))
    
    # Relationships
    region = relationship("Region", back_populates="facilities")
    specialties = relationship("FacilitySpecialty", back_populates="facility", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<MedicalFacility {self.name} ({self.city})>"
    
    @property
    def specialties_list(self):
        """Return a list of specialty names for this facility"""
        return [fs.specialty.name for fs in self.specialties]
