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
    quality_rating = db.Column(db.Float, default=None)
    
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
    quality_score = db.Column(db.Float, default=None)
    strengths_summary = db.Column(db.String(500), default=None)
    
    # Geolocation coordinates
    latitude = db.Column(db.Float, default=None)
    longitude = db.Column(db.Float, default=None)
    geocoded = db.Column(db.Boolean, default=False)
    
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
    
    @property
    def cardiology_rating(self):
        """Get cardiology rating from facility_specialty relation"""
        for fs in self.specialties:
            if fs.specialty.name.lower() == "cardiologia" and fs.quality_rating is not None:
                return fs.quality_rating
        return None
    
    @property
    def orthopedics_rating(self):
        """Get orthopedics rating from facility_specialty relation"""
        for fs in self.specialties:
            if fs.specialty.name.lower() == "ortopedia" and fs.quality_rating is not None:
                return fs.quality_rating
        return None
    
    @property
    def oncology_rating(self):
        """Get oncology rating from facility_specialty relation"""
        for fs in self.specialties:
            if fs.specialty.name.lower() == "oncologia" and fs.quality_rating is not None:
                return fs.quality_rating
        return None
    
    @property
    def neurology_rating(self):
        """Get neurology rating from facility_specialty relation"""
        for fs in self.specialties:
            if fs.specialty.name.lower() == "neurologia" and fs.quality_rating is not None:
                return fs.quality_rating
        return None
    
    @property
    def surgery_rating(self):
        """Get surgery rating from facility_specialty relation"""
        for fs in self.specialties:
            if fs.specialty.name.lower() == "chirurgia generale" and fs.quality_rating is not None:
                return fs.quality_rating
        return None
    
    @property
    def urology_rating(self):
        """Get urology rating from facility_specialty relation"""
        for fs in self.specialties:
            if fs.specialty.name.lower() == "urologia" and fs.quality_rating is not None:
                return fs.quality_rating
        return None
    
    @property
    def pediatrics_rating(self):
        """Get pediatrics rating from facility_specialty relation"""
        for fs in self.specialties:
            if fs.specialty.name.lower() == "pediatria" and fs.quality_rating is not None:
                return fs.quality_rating
        return None
    
    @property
    def gynecology_rating(self):
        """Get gynecology rating from facility_specialty relation"""
        for fs in self.specialties:
            if fs.specialty.name.lower() == "ginecologia" and fs.quality_rating is not None:
                return fs.quality_rating
        return None


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
