"""
Script per correggere i rating delle strutture problematiche identificate.

Questo script corregge manualmente i valori di rating per le strutture che
presentano discrepanze tra il database e il file CSV originale.
"""

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

# Dati CSV per le strutture da correggere
CSV_DATA = {
    'IDI - Istituto Dermopatico dell\'Immacolata': {
        'Neurologia': 1.0
    },
    'Ospedale San Carlo Borromeo': {
        'Neurologia': 3.0
    },
    'Ospedale Umberto Parini': {
        'Neurologia': 2.0
    }
}

# Dati CSV con città specifiche per strutture con lo stesso nome
CSV_DATA_WITH_CITY = [
    {
        'name': 'Ospedale San Giovanni di Dio',
        'city': 'Firenze',
        'ratings': {
            'Cardiologia': 2.0,
            'Oncologia': 1.0,
            'Pediatria': 5.0,
            'Ginecologia': 5.0
        }
    },
    {
        'name': 'Ospedale San Giovanni di Dio',
        'city': 'Crotone',
        'ratings': {
            'Cardiologia': 4.0,
            'Oncologia': 1.0,
            'Pediatria': 1.0,
            'Ginecologia': 1.0
        }
    },
    {
        'name': 'Ospedale San Giovanni di Dio',
        'city': 'Gorizia',
        'ratings': {
            'Cardiologia': 1.0,
            'Oncologia': 5.0,
            'Pediatria': 3.0,
            'Ginecologia': 2.0
        }
    }
]

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

def fix_facility_rating(facility_name, specialty_name, new_rating, city=None):
    """
    Aggiorna il rating di una specialità per una struttura
    
    Args:
        facility_name: Nome della struttura
        specialty_name: Nome della specialità
        new_rating: Nuovo valore di rating
        city: Città della struttura (opzionale)
        
    Returns:
        bool: True se l'aggiornamento è riuscito, False altrimenti
    """
    with app.app_context():
        with Session(db.engine) as session:
            # Trovo la struttura
            query = session.query(MedicalFacility).filter(
                MedicalFacility.name == facility_name
            )
            
            # Se è specificata la città, filtro anche per essa
            if city:
                query = query.filter(MedicalFacility.city == city)
                
            facility = query.first()
            
            if not facility:
                if city:
                    logger.error(f"Struttura '{facility_name}' in '{city}' non trovata")
                else:
                    logger.error(f"Struttura '{facility_name}' non trovata")
                return False
            
            # Trovo la specialità
            specialty = session.query(Specialty).filter(
                Specialty.name == specialty_name
            ).first()
            
            if not specialty:
                logger.error(f"Specialità '{specialty_name}' non trovata")
                return False
            
            # Trovo il record nel join table
            facility_specialty = session.query(FacilitySpecialty).filter(
                FacilitySpecialty.facility_id == facility.id,
                FacilitySpecialty.specialty_id == specialty.id
            ).first()
            
            if not facility_specialty:
                logger.error(f"Relazione tra '{facility_name}' e '{specialty_name}' non trovata")
                return False
            
            # Aggiorno il rating
            old_rating = facility_specialty.quality_rating
            facility_specialty.quality_rating = new_rating
            
            # Committo i cambiamenti
            session.commit()
            
            if city:
                logger.info(f"Aggiornato {facility_name} ({city}), {specialty_name}: {old_rating} -> {new_rating}")
            else:
                logger.info(f"Aggiornato {facility_name}, {specialty_name}: {old_rating} -> {new_rating}")
            return True

def main():
    """Funzione principale"""
    # Backup del database
    with app.app_context():
        backup_file = backup_database()
        logger.info(f"Backup creato: {backup_file}")
    
    # Statistiche
    stats = {
        'total': 0,
        'updated': 0,
        'failed': 0
    }
    
    # Per ogni struttura
    for facility_name, specialties in CSV_DATA.items():
        for specialty_name, rating in specialties.items():
            stats['total'] += 1
            
            if fix_facility_rating(facility_name, specialty_name, rating):
                stats['updated'] += 1
            else:
                stats['failed'] += 1
    
    # Aggiorno lo stato del database
    if stats['updated'] > 0:
        update_database_status(f"Corrette {stats['updated']} valutazioni per strutture con discrepanze")
    
    # Stampo le statistiche
    print("\n========== STATISTICHE ==========")
    print(f"Totale correzioni: {stats['total']}")
    print(f"Correzioni riuscite: {stats['updated']}")
    print(f"Correzioni fallite: {stats['failed']}")
    
    if stats['updated'] > 0:
        print("\nCorrezione completata!")
    else:
        print("\nNessuna correzione effettuata.")

if __name__ == "__main__":
    main()