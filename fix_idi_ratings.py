"""
Script per aggiungere/aggiornare tutte le valutazioni per l'IDI
(Istituto Dermopatico dell'Immacolata)

Questo script estrae le valutazioni per l'IDI dal file CSV e le aggiorna nel database.
"""

import csv
import logging
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

def get_idi_ratings(csv_file):
    """
    Estrai le valutazioni per IDI dal file CSV
    
    Args:
        csv_file: Il file CSV con le valutazioni
        
    Returns:
        dict: Dizionario con specialità -> valutazione
    """
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            facility_name = row['Name of the facility']
            if 'IDI' in facility_name:
                ratings = {}
                for csv_col, spec_name in SPECIALTY_MAPPING.items():
                    if csv_col in row and row[csv_col].strip():
                        ratings[spec_name] = float(row[csv_col])
                return ratings
    return {}

def get_specialty_id_by_name(session, name):
    """Get specialty ID by name with fuzzy matching"""
    specialty = session.query(Specialty).filter(
        Specialty.name.ilike(f"%{name}%")
    ).first()
    
    if specialty:
        return specialty.id
    return None

def update_idi_ratings(csv_file):
    """
    Aggiorna tutte le valutazioni per IDI dal file CSV
    
    Args:
        csv_file: Il file CSV con le valutazioni
    """
    # Estrai le valutazioni per IDI dal CSV
    ratings = get_idi_ratings(csv_file)
    if not ratings:
        logger.error("Nessuna valutazione trovata per IDI nel CSV")
        return
    
    logger.info(f"Trovate {len(ratings)} valutazioni per IDI: {ratings}")
    
    # Utilizziamo il contesto dell'applicazione per tutto il processo
    with app.app_context():
        # Prima, crea un backup del database
        backup_file = backup_database()
        logger.info(f"Backup creato: {backup_file}")
        
        with Session(db.engine) as session:
            # Trova l'ID della struttura IDI
            idi = session.query(MedicalFacility).filter(
                MedicalFacility.name.ilike("%IDI%")
            ).first()
            
            if not idi:
                logger.error("Struttura IDI non trovata nel database")
                return
            
            idi_id = idi.id
            logger.info(f"Trovata struttura IDI con ID {idi_id}: {idi.name}")
            
            # Per ogni specialità nel mapping
            for specialty_name, rating in ratings.items():
                # Trova l'ID della specialità
                specialty_id = get_specialty_id_by_name(session, specialty_name)
                
                if not specialty_id:
                    logger.warning(f"Specialità non trovata: {specialty_name}")
                    continue
                
                # Verifica se esiste già un'associazione
                existing = session.query(FacilitySpecialty).filter_by(
                    facility_id=idi_id,
                    specialty_id=specialty_id
                ).first()
                
                if existing:
                    # Aggiorna la valutazione esistente
                    existing.quality_rating = rating
                    logger.info(f"Aggiornata valutazione per {specialty_name}: {rating}")
                else:
                    # Crea una nuova associazione
                    new_rating = FacilitySpecialty(
                        facility_id=idi_id,
                        specialty_id=specialty_id,
                        quality_rating=rating
                    )
                    session.add(new_rating)
                    logger.info(f"Aggiunta valutazione per {specialty_name}: {rating}")
            
            # Salva le modifiche
            session.commit()
            
            # Aggiorna lo stato del database
            update_database_status("Aggiornate valutazioni per IDI")
            
            logger.info("Aggiornamento completato per IDI")

def update_database_status(message):
    """Aggiorna lo stato del database"""
    try:
        # Assicuriamoci di essere nel contesto dell'applicazione
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

if __name__ == "__main__":
    update_idi_ratings("./attached_assets/medical_facilities_full_ratings.csv")
    print("Aggiornamento completato per le valutazioni di IDI.")