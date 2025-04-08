import os
import logging
import pandas as pd
import urllib.request
from io import StringIO
import csv
from app import db
from models import MedicalFacility, Specialty, FacilitySpecialty, Region
from sqlalchemy.exc import IntegrityError
import re
from unidecode import unidecode

logger = logging.getLogger(__name__)

# Data sources with attributions
DATA_SOURCES = {
    'puglia': {
        'url': 'https://www.dati.puglia.it/dataset/5925f2fc-c244-4f1a-b55e-96fcaf410192/resource/31e71b2e-01d8-484c-8c2e-e43a9907d19e/download/anagrafe_strutture_sanitarie.csv',
        'attribution': 'Regione Puglia - Anagrafe strutture sanitarie - IODL 2.0',
        'region_name': 'Puglia'
    },
    'trento': {
        'url': 'https://dati.trentino.it/dataset/c3c18874-94f3-4c82-ab75-a557f605cfef/resource/9087e4b4-d6e5-4620-9a5a-d5e1f8d5558d/download/elencostrutturesanitarieaggiornato.csv',
        'attribution': 'Provincia Autonoma di Trento - Strutture sanitarie - CC-BY',
        'region_name': 'Trentino'
    },
    'toscana': {
        'url': 'https://www.opendata.toscana.it/dataset/3e05970d-a3eb-4b02-83a3-12025e4485e5/resource/6bc90d77-88ee-42bf-89ee-8a8aa2df641a/download/strutture.ospedaliere.csv',
        'attribution': 'Regione Toscana - Strutture ospedaliere - IODL 2.0',
        'region_name': 'Toscana'
    }
}

# Specialty mapping to normalize across regions
SPECIALTY_MAPPING = {
    'cardiologia': 'Cardiologia',
    'cardio': 'Cardiologia',
    'cardiolog': 'Cardiologia',
    'ortopedia': 'Ortopedia',
    'ortoped': 'Ortopedia',
    'traumatologia': 'Ortopedia e Traumatologia',
    'pediatria': 'Pediatria',
    'pediatr': 'Pediatria',
    'medicina generale': 'Medicina Generale',
    'medicina interna': 'Medicina Interna',
    'ginecologia': 'Ginecologia e Ostetricia',
    'ostetricia': 'Ginecologia e Ostetricia',
    'ginecolog': 'Ginecologia e Ostetricia',
    'ostetric': 'Ginecologia e Ostetricia',
    'neurologia': 'Neurologia',
    'neurolog': 'Neurologia',
    'psichiatria': 'Psichiatria',
    'psichiatr': 'Psichiatria',
    'dermatologia': 'Dermatologia',
    'dermatolog': 'Dermatologia',
    'oculistica': 'Oculistica',
    'oftalmolog': 'Oculistica',
    'oculist': 'Oculistica',
    'otorino': 'Otorinolaringoiatria',
    'otorinolaringoiatria': 'Otorinolaringoiatria',
    'urologia': 'Urologia',
    'urolog': 'Urologia',
    'oncologia': 'Oncologia',
    'oncolog': 'Oncologia',
    'radiologia': 'Radiologia',
    'radiolog': 'Radiologia',
    'diagnostica': 'Diagnostica per Immagini',
    'laboratorio': 'Analisi Cliniche',
    'analisi': 'Analisi Cliniche',
    'pronto soccorso': 'Pronto Soccorso',
    'emergenza': 'Pronto Soccorso',
    'ambulatorio': 'Ambulatorio',
    'fisioterapia': 'Fisioterapia',
    'riabilitazione': 'Riabilitazione',
    'riabilit': 'Riabilitazione',
    'fisioter': 'Fisioterapia'
}

def normalize_specialty(specialty_name):
    """Normalize specialty names to standard format"""
    if not specialty_name:
        return None
    
    # Convert to lowercase, remove accents, and strip whitespace
    normalized = unidecode(specialty_name.lower().strip())
    
    # Check if it matches any key in the mapping
    for key, value in SPECIALTY_MAPPING.items():
        if key in normalized:
            return value
    
    # If no match found, return capitalized version of original
    return specialty_name.capitalize()

def safe_get(df, row_idx, col_name, default=None):
    """Safely get a value from a dataframe with error handling"""
    try:
        value = df.iloc[row_idx][col_name]
        if pd.isna(value) or value == '':
            return default
        return value
    except (KeyError, IndexError):
        return default

def get_or_create_region(region_name):
    """Get or create a region by name"""
    if not region_name:
        return None
    
    region = Region.query.filter_by(name=region_name).first()
    if not region:
        region = Region(name=region_name)
        db.session.add(region)
        db.session.commit()
    return region

def get_or_create_specialty(specialty_name):
    """Get or create a specialty by name"""
    if not specialty_name:
        return None
    
    normalized_name = normalize_specialty(specialty_name)
    specialty = Specialty.query.filter_by(name=normalized_name).first()
    if not specialty:
        specialty = Specialty(name=normalized_name)
        db.session.add(specialty)
        db.session.commit()
    return specialty

def extract_specialties(text):
    """Extract multiple specialties from text"""
    if not text or not isinstance(text, str):
        return []
    
    # Split by common separators and clean up
    specialties = []
    separators = [',', ';', '/', '|', 'e', 'ed', 'and', '-']
    
    # First try to split by separators
    parts = [text]
    for sep in separators:
        new_parts = []
        for part in parts:
            new_parts.extend([p.strip() for p in part.split(sep) if p.strip()])
        parts = new_parts
    
    # Then try to match with known specialties
    for part in parts:
        norm_part = normalize_specialty(part)
        if norm_part:
            specialties.append(norm_part)
    
    # Deduplicate
    return list(set(specialties))

def download_csv(url):
    """Download a CSV file from a URL and return a pandas DataFrame"""
    try:
        logger.info(f"Downloading data from {url}")
        response = urllib.request.urlopen(url)
        content = response.read().decode('utf-8', errors='ignore')
        
        # Try to determine the delimiter
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(content[:1024])
        delimiter = dialect.delimiter
        
        return pd.read_csv(StringIO(content), delimiter=delimiter, low_memory=False)
    except Exception as e:
        logger.error(f"Error downloading or parsing CSV from {url}: {str(e)}")
        raise

def load_puglia_data(data_source):
    """Load and process data from Puglia region"""
    logger.info("Processing Puglia data")
    df = download_csv(data_source['url'])
    
    region = get_or_create_region(data_source['region_name'])
    facilities_added = 0
    
    for idx in range(len(df)):
        # Extract basic facility information
        name = safe_get(df, idx, 'DENOMSTRUTTURA')
        if not name:
            continue
            
        facility_type = safe_get(df, idx, 'TIPOLOGIASTRUTTURA')
        address = safe_get(df, idx, 'INDIRIZZO')
        city = safe_get(df, idx, 'COMUNE')
        
        # Look for existing facility to avoid duplicates
        existing = MedicalFacility.query.filter_by(
            name=name, 
            city=city
        ).first()
        
        if existing:
            logger.debug(f"Facility already exists: {name} in {city}")
            continue
        
        # Create new facility
        facility = MedicalFacility(
            name=name,
            address=address,
            city=city,
            region=region,
            facility_type=facility_type,
            telephone=safe_get(df, idx, 'TELEFONO'),
            data_source="Puglia Open Data",
            attribution=data_source['attribution'],
            # Set default values for optional fields
            quality_score=3.0,  # Placeholder
            cost_estimate=None
        )
        
        # Add facility to database
        db.session.add(facility)
        db.session.commit()
        
        # Process specialties
        specialties_text = safe_get(df, idx, 'BRANCHEAUTORIZZATE')
        if specialties_text:
            specialty_names = extract_specialties(specialties_text)
            for specialty_name in specialty_names:
                specialty = get_or_create_specialty(specialty_name)
                if specialty:
                    facility_specialty = FacilitySpecialty(
                        facility_id=facility.id,
                        specialty_id=specialty.id
                    )
                    db.session.add(facility_specialty)
        
        db.session.commit()
        facilities_added += 1
    
    return facilities_added

def load_trento_data(data_source):
    """Load and process data from Trento region"""
    logger.info("Processing Trento data")
    df = download_csv(data_source['url'])
    
    region = get_or_create_region(data_source['region_name'])
    facilities_added = 0
    
    for idx in range(len(df)):
        # Extract basic facility information
        name = safe_get(df, idx, 'DENOMINAZIONE')
        if not name:
            continue
            
        facility_type = safe_get(df, idx, 'TIPO')
        address = safe_get(df, idx, 'INDIRIZZO')
        city = safe_get(df, idx, 'COMUNE')
        
        # Look for existing facility to avoid duplicates
        existing = MedicalFacility.query.filter_by(
            name=name, 
            city=city
        ).first()
        
        if existing:
            logger.debug(f"Facility already exists: {name} in {city}")
            continue
        
        # Create new facility
        facility = MedicalFacility(
            name=name,
            address=address,
            city=city,
            region=region,
            facility_type=facility_type,
            telephone=safe_get(df, idx, 'TELEFONO'),
            email=safe_get(df, idx, 'EMAIL'),
            website=safe_get(df, idx, 'SITO WEB'),
            data_source="Trento Open Data",
            attribution=data_source['attribution'],
            # Set default values for optional fields
            quality_score=3.5,  # Placeholder
            cost_estimate=None
        )
        
        # Add facility to database
        db.session.add(facility)
        db.session.commit()
        
        # Process specialties
        specialties_text = safe_get(df, idx, 'PRESTAZIONI')
        if specialties_text:
            specialty_names = extract_specialties(specialties_text)
            for specialty_name in specialty_names:
                specialty = get_or_create_specialty(specialty_name)
                if specialty:
                    facility_specialty = FacilitySpecialty(
                        facility_id=facility.id,
                        specialty_id=specialty.id
                    )
                    db.session.add(facility_specialty)
        
        db.session.commit()
        facilities_added += 1
    
    return facilities_added

def load_toscana_data(data_source):
    """Load and process data from Toscana region"""
    logger.info("Processing Toscana data")
    df = download_csv(data_source['url'])
    
    region = get_or_create_region(data_source['region_name'])
    facilities_added = 0
    
    for idx in range(len(df)):
        # Extract basic facility information
        name = safe_get(df, idx, 'Denominazione')
        if not name:
            continue
            
        facility_type = "Ospedale"  # Default for Toscana dataset
        address = safe_get(df, idx, 'Indirizzo')
        city = safe_get(df, idx, 'Comune')
        
        # Look for existing facility to avoid duplicates
        existing = MedicalFacility.query.filter_by(
            name=name, 
            city=city
        ).first()
        
        if existing:
            logger.debug(f"Facility already exists: {name} in {city}")
            continue
        
        # Create new facility
        facility = MedicalFacility(
            name=name,
            address=address,
            city=city,
            region=region,
            facility_type=facility_type,
            telephone=safe_get(df, idx, 'Telefono'),
            data_source="Toscana Open Data",
            attribution=data_source['attribution'],
            # Set default values for optional fields
            quality_score=4.0,  # Placeholder
            cost_estimate=None
        )
        
        # Add facility to database
        db.session.add(facility)
        db.session.commit()
        
        # Process specialties - for Toscana we might need to infer from the name
        specialties_text = safe_get(df, idx, 'Tipologia')
        if specialties_text:
            specialty_names = extract_specialties(specialties_text)
            for specialty_name in specialty_names:
                specialty = get_or_create_specialty(specialty_name)
                if specialty:
                    facility_specialty = FacilitySpecialty(
                        facility_id=facility.id,
                        specialty_id=specialty.id
                    )
                    db.session.add(facility_specialty)
        
        db.session.commit()
        facilities_added += 1
    
    return facilities_added

def load_data():
    """Load data from all sources"""
    stats = {
        'regions': 0,
        'total': 0
    }
    
    # Load data from each source
    loaders = {
        'puglia': load_puglia_data,
        'trento': load_trento_data,
        'toscana': load_toscana_data
    }
    
    for source_key, loader_func in loaders.items():
        try:
            source = DATA_SOURCES[source_key]
            count = loader_func(source)
            logger.info(f"Added {count} facilities from {source['region_name']}")
            
            if count > 0:
                stats['regions'] += 1
                stats['total'] += count
                
        except Exception as e:
            logger.error(f"Error loading data from {source_key}: {str(e)}")
    
    return stats

def get_regions():
    """Get all regions from the database"""
    return Region.query.order_by(Region.name).all()

def get_specialties():
    """Get all specialties from the database"""
    return Specialty.query.order_by(Specialty.name).all()
