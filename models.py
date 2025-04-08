from app import db
from sqlalchemy import Column, Integer, String, Float, ForeignKey, Table, DateTime
from sqlalchemy.orm import relationship
import datetime

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
    
    # Specialty ratings (1-5 scale)
    cardiology_rating = db.Column(db.Float, default=None)
    orthopedics_rating = db.Column(db.Float, default=None)
    oncology_rating = db.Column(db.Float, default=None)
    neurology_rating = db.Column(db.Float, default=None)
    surgery_rating = db.Column(db.Float, default=None)
    urology_rating = db.Column(db.Float, default=None)
    pediatrics_rating = db.Column(db.Float, default=None)
    gynecology_rating = db.Column(db.Float, default=None)
    
    # Strengths summary
    strengths_summary = db.Column(db.String(500), default=None)
    
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


class DatabaseStatus(db.Model):
    """
    Tracks the status of database initialization and updates.
    This allows us to load data only once and then use it for all subsequent requests.
    """
    __tablename__ = 'database_status'
    
    id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.String(50), nullable=False)  # initialized, updating, error
    last_updated = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    total_facilities = db.Column(db.Integer, default=0)
    total_regions = db.Column(db.Integer, default=0)
    total_specialties = db.Column(db.Integer, default=0)
    initialized_by = db.Column(db.String(100))
    notes = db.Column(db.String(500))
    
    @classmethod
    def get_status(cls):
        """Get the current database status"""
        return cls.query.order_by(cls.last_updated.desc()).first()
    
    @classmethod
    def update_status(cls, status, total_facilities=None, total_regions=None, 
                     total_specialties=None, notes=None, initialized_by=None):
        """Update the database status"""
        new_status = cls(
            status=status,
            last_updated=datetime.datetime.utcnow(),
            total_facilities=total_facilities,
            total_regions=total_regions,
            total_specialties=total_specialties,
            notes=notes,
            initialized_by=initialized_by
        )
        db.session.add(new_status)
        db.session.commit()
        return new_status
