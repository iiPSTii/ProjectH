"""
Script per aggiungere manualmente le valutazioni di Urologia alle strutture rimanenti

Questo script aggiunge le valutazioni di Urologia alle 5 strutture che non sono state
importate tramite i precedenti script.
"""

import csv
import logging
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import Session
from app import app, db
from models import MedicalFacility, Specialty, FacilitySpecialty

# Setup logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Mappatura manuale delle strutture rimanenti con i nomi nel CSV
facility_manual_mapping = {
    "Ospedale Morgagni - Pierantoni": "Ospedale Morgagni",
    "Ospedale di Gubbio - Gualdo Tadino": "Ospedale di Gubbio",
    "IDI - Istituto Dermopatico dell'Immacolata": "IDI",
    "AORN Dei Colli - Monaldi": "AORN Dei Colli",
    "AORN Dei Colli - Cotugno": "AORN Dei Colli"
}

def find_urologia_ratings(csv_file):
    """
    Estrai le valutazioni di Urologia dal file CSV
    
    Args:
        csv_file: Il file CSV con le valutazioni
        
    Returns:
        dict: Dizionario con nome struttura -> valutazione
    """
    ratings = {}
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            facility_name = row['Name of the facility']
            urologia_rating = row['Urologia'].strip()
            if urologia_rating:
                ratings[facility_name] = float(urologia_rating)
    
    return ratings

def add_remaining_urologia_ratings(csv_file):
    """
    Aggiungi le valutazioni di Urologia alle strutture rimanenti
    
    Args:
        csv_file: Il file CSV con le valutazioni
    """
    # Estrai le valutazioni di Urologia dal CSV
    ratings = find_urologia_ratings(csv_file)
    
    # ID della specialità Urologia
    urologia_id = 791
    
    with app.app_context():
        with Session(db.engine) as session:
            # Verifica che l'ID di Urologia sia corretto
            specialty = session.query(Specialty).filter_by(id=urologia_id).first()
            if not specialty or specialty.name != 'Urologia':
                logger.error(f"Specialità Urologia non trovata con ID {urologia_id}")
                return
            
            # Elabora ogni struttura nella mappatura manuale
            for db_name, csv_name in facility_manual_mapping.items():
                # Cerca la struttura nel database
                facility = session.query(MedicalFacility).filter(
                    MedicalFacility.name.ilike(f"{db_name}%")
                ).first()
                
                if not facility:
                    logger.error(f"Struttura non trovata: {db_name}")
                    continue
                
                # Cerca la valutazione nel CSV
                if csv_name not in ratings:
                    logger.error(f"Valutazione non trovata per: {csv_name}")
                    continue
                
                rating = ratings[csv_name]
                
                # Controlla se esiste già una valutazione per questa struttura
                existing = session.query(FacilitySpecialty).filter_by(
                    facility_id=facility.id,
                    specialty_id=urologia_id
                ).first()
                
                if existing:
                    logger.info(f"La struttura {db_name} ha già una valutazione per Urologia: {existing.quality_rating}")
                    continue
                
                # Crea una nuova associazione
                new_rating = FacilitySpecialty(
                    facility_id=facility.id,
                    specialty_id=urologia_id,
                    quality_rating=rating
                )
                session.add(new_rating)
                logger.info(f"Aggiunta valutazione {rating} per Urologia a {db_name} (ID: {facility.id})")
            
            # Salva le modifiche
            session.commit()
            
            # Aggiorna lo stato del database
            update_database_status("Aggiunte valutazioni di Urologia alle ultime 5 strutture")

def update_database_status(message):
    """Aggiorna lo stato del database"""
    try:
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
    add_remaining_urologia_ratings("./attached_assets/medical_facilities_full_ratings.csv")
    print("Completata l'aggiunta delle valutazioni di Urologia per le strutture rimanenti.")