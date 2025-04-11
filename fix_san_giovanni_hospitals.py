"""
Script per correggere i valori degli ospedali San Giovanni di Dio.

Questo script corregge i valori delle specialità per i tre ospedali San Giovanni di Dio
in base ai dati nel CSV.
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

# Dati corretti dal CSV per i tre ospedali
CORRECT_DATA = [
    {
        'name': 'Ospedale San Giovanni di Dio',
        'city': 'Firenze',
        'specialties': {
            'Cardiologia': 2.0,
            'Ortopedia': 4.0,
            'Oncologia': 1.0,
            'Neurologia': 4.0,
            'Urologia': 3.0,
            'Chirurgia Generale': 5.0,
            'Pediatria': 5.0,
            'Ginecologia': 5.0
        }
    },
    {
        'name': 'Ospedale San Giovanni di Dio',
        'city': 'Crotone',
        'specialties': {
            'Cardiologia': 4.0,
            'Ortopedia': 3.0,
            'Oncologia': 1.0,
            'Neurologia': 4.0,
            'Urologia': 3.0,
            'Chirurgia Generale': 3.0,
            'Pediatria': 1.0,
            'Ginecologia': 1.0
        }
    },
    {
        'name': 'Ospedale San Giovanni di Dio',
        'city': 'Gorizia',
        'specialties': {
            'Cardiologia': 1.0,
            'Ortopedia': 4.0,
            'Oncologia': 5.0,
            'Neurologia': 3.0,
            'Urologia': 3.0,
            'Chirurgia Generale': 5.0,
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

def fix_hospital_ratings():
    """
    Corregge i rating per i tre ospedali San Giovanni di Dio
    
    Returns:
        dict: Statistiche sulle correzioni
    """
    # Statistiche
    stats = {
        'total': 0,
        'updated': 0,
        'failed': 0,
        'hospitals_updated': 0
    }
    
    # Backup del database
    with app.app_context():
        backup_file = backup_database()
        logger.info(f"Backup creato: {backup_file}")
    
    # Per ogni ospedale
    for hospital_data in CORRECT_DATA:
        name = hospital_data['name']
        city = hospital_data['city']
        
        # Trovo l'ospedale nel database
        with app.app_context():
            with Session(db.engine) as session:
                hospital = session.query(MedicalFacility).filter(
                    MedicalFacility.name == name,
                    MedicalFacility.city == city
                ).first()
                
                if not hospital:
                    logger.error(f"Ospedale '{name}' in '{city}' non trovato nel database")
                    continue
                
                # Flag per verificare se l'ospedale è stato aggiornato
                hospital_updated = False
                
                # Per ogni specialità
                for specialty_name, correct_value in hospital_data['specialties'].items():
                    stats['total'] += 1
                    
                    # Trovo la specialità
                    specialty = session.query(Specialty).filter(
                        Specialty.name == specialty_name
                    ).first()
                    
                    if not specialty:
                        logger.error(f"Specialità '{specialty_name}' non trovata nel database")
                        stats['failed'] += 1
                        continue
                    
                    # Trovo il record nel join table
                    facility_specialty = session.query(FacilitySpecialty).filter(
                        FacilitySpecialty.facility_id == hospital.id,
                        FacilitySpecialty.specialty_id == specialty.id
                    ).first()
                    
                    if not facility_specialty:
                        logger.error(f"Relazione tra '{name}' ({city}) e '{specialty_name}' non trovata")
                        stats['failed'] += 1
                        continue
                    
                    # Verifico se il valore è già corretto
                    if abs(facility_specialty.quality_rating - correct_value) < 0.01:
                        logger.info(f"Valore già corretto per '{name}' ({city}), '{specialty_name}': {correct_value}")
                        continue
                    
                    # Aggiorno il rating
                    old_value = facility_specialty.quality_rating
                    facility_specialty.quality_rating = correct_value
                    
                    logger.info(f"Aggiornato '{name}' ({city}), '{specialty_name}': {old_value} -> {correct_value}")
                    stats['updated'] += 1
                    hospital_updated = True
                
                # Salvo le modifiche
                if hospital_updated:
                    session.commit()
                    stats['hospitals_updated'] += 1
    
    # Aggiorno lo stato del database
    if stats['updated'] > 0:
        update_database_status(f"Corretti {stats['updated']} valori di specialità per {stats['hospitals_updated']} ospedali 'San Giovanni di Dio'")
    
    return stats

def print_stats(stats):
    """
    Stampa le statistiche delle correzioni
    
    Args:
        stats: Statistiche
    """
    print("\n========== STATISTICHE ==========")
    print(f"Valori totali: {stats['total']}")
    print(f"Valori aggiornati: {stats['updated']}")
    print(f"Aggiornamenti falliti: {stats['failed']}")
    print(f"Ospedali aggiornati: {stats['hospitals_updated']}")
    
    if stats['updated'] > 0:
        print("\nCorrezione completata!")
    else:
        print("\nNessuna correzione necessaria.")

if __name__ == "__main__":
    print("Correzione dei valori per gli ospedali San Giovanni di Dio...")
    
    # Eseguo la correzione
    stats = fix_hospital_ratings()
    
    # Stampo le statistiche
    print_stats(stats)