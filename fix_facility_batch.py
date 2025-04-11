"""
Script per aggiornare le valutazioni di una o più strutture specifiche

Questo script è una versione più leggera di fix_multiple_facilities_ratings.py,
progettato per elaborare una o poche strutture alla volta ed evitare timeout.
"""

import csv
import logging
import sys
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import Session
from app import app, db
from models import MedicalFacility, Specialty, FacilitySpecialty
from backup_database import backup_database

# Setup logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Mapping delle colonne nel CSV alle specialità
SPECIALTY_MAPPING = {
    'Cardiologia': 'Cardiologia',
    'Ortopedia': 'Ortopedia',
    'Oncologia': 'Oncologia',
    'Neurologia': 'Neurologia',
    'Urologia': 'Urologia',
    'Chirurgia generale': 'Chirurgia Generale',
    'Pediatria': 'Pediatria',
    'Ginecologia': 'Ginecologia'
}

def load_csv_data(csv_file):
    """
    Carica i dati dal file CSV
    
    Args:
        csv_file: Il percorso del file CSV
        
    Returns:
        dict: Dizionario con nome struttura -> dati
    """
    facilities_data = {}
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            facility_name = row['Name of the facility']
            # Converto i dati delle specialità in float, ignorando valori non validi
            for specialty in SPECIALTY_MAPPING.keys():
                if specialty in row and row[specialty].strip():
                    try:
                        row[specialty] = float(row[specialty])
                    except ValueError:
                        logger.warning(f"Valore non valido per {facility_name}, {specialty}: {row[specialty]}")
                        row[specialty] = ""
            
            facilities_data[facility_name] = row
    
    return facilities_data

def get_specialty_id_by_name(session, name):
    """Get specialty ID by name with fuzzy matching"""
    specialty = session.query(Specialty).filter(
        Specialty.name.ilike(f"%{name}%")
    ).first()
    
    if specialty:
        return specialty.id
    return None

def get_facility_by_exact_name(session, name):
    """Get facility by exact name"""
    facility = session.query(MedicalFacility).filter(
        MedicalFacility.name == name
    ).first()
    
    return facility

def get_facility_by_partial_name(session, name):
    """Get facility by partial name match"""
    facility = session.query(MedicalFacility).filter(
        MedicalFacility.name.ilike(f"%{name}%")
    ).first()
    
    return facility

def update_facility_ratings(facility, csv_data, session, specialty_ids):
    """
    Aggiorna le valutazioni per una singola struttura
    
    Args:
        facility: L'oggetto MedicalFacility
        csv_data: I dati CSV per questa struttura
        session: La sessione del database
        specialty_ids: Dizionario specialty_name -> specialty_id
        
    Returns:
        dict: Statistiche sugli aggiornamenti
    """
    stats = {'updated': 0, 'created': 0, 'skipped': 0}
    
    for csv_spec_name, db_spec_name in SPECIALTY_MAPPING.items():
        # Verifichiamo che ci sia un valore valido per la specialità
        if csv_spec_name not in csv_data:
            logger.debug(f"Specialità {csv_spec_name} non presente nel CSV per {facility.name}")
            stats['skipped'] += 1
            continue
        
        # Se il valore è una stringa vuota, lo saltiamo
        if isinstance(csv_data[csv_spec_name], str) and not csv_data[csv_spec_name].strip():
            logger.debug(f"Valore vuoto per {facility.name}, {csv_spec_name}")
            stats['skipped'] += 1
            continue
            
        # A questo punto, il valore dovrebbe essere un numero o una stringa numerica
        try:
            if isinstance(csv_data[csv_spec_name], str):
                rating = float(csv_data[csv_spec_name])
            else:
                rating = csv_data[csv_spec_name]  # Già convertito in float
        except (ValueError, TypeError):
            logger.warning(f"Impossibile convertire in float: {csv_data[csv_spec_name]} per {facility.name}, {csv_spec_name}")
            stats['skipped'] += 1
            continue
        
        # Assicuriamoci che il rating sia nel range corretto
        rating = max(1.0, min(5.0, rating))
        
        if db_spec_name not in specialty_ids:
            logger.warning(f"Specialità {db_spec_name} non trovata nel database")
            stats['skipped'] += 1
            continue
            
        specialty_id = specialty_ids[db_spec_name]
        
        # Verifica se esiste già la relazione
        existing = session.query(FacilitySpecialty).filter_by(
            facility_id=facility.id,
            specialty_id=specialty_id
        ).first()
        
        if existing:
            # Aggiorna il punteggio esistente
            old_rating = existing.quality_rating
            existing.quality_rating = rating
            logger.info(f"Aggiornato {facility.name}, {db_spec_name}: {old_rating} -> {rating}")
            stats['updated'] += 1
        else:
            # Crea una nuova relazione
            new_rating = FacilitySpecialty(
                facility_id=facility.id,
                specialty_id=specialty_id,
                quality_rating=rating
            )
            session.add(new_rating)
            logger.info(f"Creato {facility.name}, {db_spec_name}: {rating}")
            stats['created'] += 1
    
    return stats

def update_database_status(message):
    """Aggiorna lo stato del database"""
    try:
        with app.app_context():
            with Session(db.engine) as session:
                timestamp = datetime.now()
                session.execute(
                    text("UPDATE database_status SET status = :status, last_updated = :timestamp"),
                    {"status": "updated", "timestamp": timestamp}
                )
                session.execute(
                    text("INSERT INTO database_status_log (status, message, timestamp) VALUES (:status, :message, :timestamp)"),
                    {"status": "updated", "message": message, "timestamp": timestamp}
                )
                session.commit()
                logger.info(f"Aggiornato stato del database: {message}")
    except Exception as e:
        logger.error(f"Errore nell'aggiornamento dello stato del database: {e}")

def fix_single_facility(db_name, csv_name=None):
    """
    Aggiorna le valutazioni per una singola struttura
    
    Args:
        db_name: Il nome della struttura nel database
        csv_name: Il nome della struttura nel CSV (se diverso da db_name)
    """
    csv_file = "./attached_assets/medical_facilities_full_ratings.csv"
    csv_data = load_csv_data(csv_file)
    
    if csv_name is None:
        csv_name = db_name
    
    if csv_name not in csv_data:
        print(f"Errore: struttura '{csv_name}' non trovata nel file CSV.")
        return
    
    facility_data = csv_data[csv_name]
    print(f"Struttura trovata nel CSV: {csv_name}")
    
    # Elaboro la struttura
    with app.app_context():
        with Session(db.engine) as session:
            # Cerco la struttura nel database
            facility = get_facility_by_exact_name(session, db_name)
            if not facility:
                facility = get_facility_by_partial_name(session, db_name)
                if not facility:
                    print(f"Errore: struttura '{db_name}' non trovata nel database.")
                    return
            
            print(f"Struttura trovata nel database: {facility.name} (ID: {facility.id})")
            
            # Ottengo gli ID delle specialità
            specialty_ids = {}
            for _, spec_name in SPECIALTY_MAPPING.items():
                specialty_id = get_specialty_id_by_name(session, spec_name)
                if specialty_id:
                    specialty_ids[spec_name] = specialty_id
                else:
                    logger.warning(f"Specialità {spec_name} non trovata nel database")
            
            # Backup del database prima di aggiornare
            backup_file = backup_database()
            print(f"Backup creato: {backup_file}")
            
            # Aggiorno le valutazioni
            stats = update_facility_ratings(facility, facility_data, session, specialty_ids)
            
            # Salvo le modifiche
            session.commit()
            update_database_status(f"Aggiornate valutazioni per {facility.name}")
            
            # Statistiche
            print(f"\nAggiornamento completato per {facility.name}:")
            print(f"  Valutazioni aggiornate: {stats['updated']}")
            print(f"  Valutazioni create: {stats['created']}")
            print(f"  Valutazioni saltate: {stats['skipped']}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Utilizzo: python fix_facility_batch.py <nome_struttura_db> [nome_struttura_csv]")
        print("Esempio: python fix_facility_batch.py \"Ospedale San Paolo\" \"Ospedale San Paolo\"")
        sys.exit(1)
    
    db_name = sys.argv[1]
    csv_name = sys.argv[2] if len(sys.argv) > 2 else db_name
    
    print(f"Aggiornamento valutazioni per {db_name} (dal CSV: {csv_name})...")
    fix_single_facility(db_name, csv_name)