"""
Script per correggere l'ordine delle colonne nel processo di importazione

Questo script aggiorna i valori di una struttura tenendo conto dell'ordine esatto
delle colonne nel file CSV originale.
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

def load_csv_data(csv_file, facility_name):
    """
    Carica i dati di una specifica struttura dal file CSV
    
    Args:
        csv_file: Il percorso del file CSV
        facility_name: Nome della struttura da cercare
        
    Returns:
        dict: Dizionario con colonna -> valore
    """
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['Name of the facility'] == facility_name:
                # Converto i dati in float dove possibile
                for key, value in row.items():
                    if key not in ['Name of the facility', 'City'] and value.strip():
                        try:
                            row[key] = float(value)
                        except ValueError:
                            logger.warning(f"Valore non valido per {facility_name}, {key}: {value}")
                return row
    return None

def update_facility_ratings(facility_id, csv_data):
    """
    Aggiorna le valutazioni per una struttura usando i dati originali CSV
    
    Args:
        facility_id: ID della struttura nel database
        csv_data: Dati CSV per questa struttura
    """
    with app.app_context():
        # Prima creiamo un backup del database
        backup_file = backup_database()
        logger.info(f"Backup creato: {backup_file}")
        
        with Session(db.engine) as session:
            # Otteniamo la struttura
            facility = session.query(MedicalFacility).get(facility_id)
            if not facility:
                logger.error(f"Struttura con ID {facility_id} non trovata")
                return
            
            logger.info(f"Aggiornamento valutazioni per {facility.name} (ID: {facility_id})")
            
            # Mappatura diretta colonna CSV -> specialità
            # Le colonne sono in questo ordine: Cardiologia,Ortopedia,Oncologia,Neurologia,Urologia,Chirurgia generale,Pediatria,Ginecologia
            for csv_column, value in csv_data.items():
                # Saltiamo le colonne non specialità
                if csv_column in ['Name of the facility', 'City']:
                    continue
                
                # Convertiamo il nome della colonna CSV al nome della specialità
                specialty_name = csv_column
                
                # Chirurgia generale -> Chirurgia Generale 
                if specialty_name.lower() == 'chirurgia generale':
                    specialty_name = 'Chirurgia Generale'
                
                # Troviamo la specialità
                specialty = session.query(Specialty).filter(
                    Specialty.name.ilike(f"%{specialty_name}%")
                ).first()
                
                if not specialty:
                    logger.warning(f"Specialità {specialty_name} non trovata")
                    continue
                
                # Verifichiamo se abbiamo un valore valido
                if not isinstance(value, (int, float)):
                    try:
                        if isinstance(value, str) and value.strip():
                            value = float(value)
                        else:
                            logger.warning(f"Valore non valido per {facility.name}, {specialty_name}: {value}")
                            continue
                    except (ValueError, TypeError):
                        logger.warning(f"Impossibile convertire in float: {value}")
                        continue
                
                # Assicuriamoci che il rating sia nel range corretto
                rating = max(1.0, min(5.0, value))
                
                # Verifica se esiste già la relazione
                existing = session.query(FacilitySpecialty).filter_by(
                    facility_id=facility.id,
                    specialty_id=specialty.id
                ).first()
                
                if existing:
                    # Aggiorna il punteggio esistente
                    old_rating = existing.quality_rating
                    existing.quality_rating = rating
                    logger.info(f"Aggiornato {facility.name}, {specialty.name}: {old_rating} -> {rating}")
                else:
                    # Crea una nuova relazione
                    new_rating = FacilitySpecialty(
                        facility_id=facility.id,
                        specialty_id=specialty.id,
                        quality_rating=rating
                    )
                    session.add(new_rating)
                    logger.info(f"Creato {facility.name}, {specialty.name}: {rating}")
            
            # Salviamo le modifiche
            session.commit()
            update_database_status(f"Corrette valutazioni per {facility.name}")

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

def fix_neurologia_value(facility_id, rating_value):
    """
    Aggiorna manualmente il valore della specialità Neurologia
    
    Args:
        facility_id: ID della struttura
        rating_value: Nuovo valore di rating
    """
    with app.app_context():
        # Prima creiamo un backup del database
        backup_file = backup_database()
        logger.info(f"Backup creato: {backup_file}")
        
        with Session(db.engine) as session:
            # Otteniamo la struttura
            facility = session.query(MedicalFacility).get(facility_id)
            if not facility:
                logger.error(f"Struttura con ID {facility_id} non trovata")
                return
            
            # Otteniamo la specialità
            specialty = session.query(Specialty).filter(
                Specialty.name == 'Neurologia'
            ).first()
            
            if not specialty:
                logger.error("Specialità Neurologia non trovata nel database")
                return
            
            # Aggiorniamo il rating
            facility_specialty = session.query(FacilitySpecialty).filter_by(
                facility_id=facility_id,
                specialty_id=specialty.id
            ).first()
            
            if facility_specialty:
                old_rating = facility_specialty.quality_rating
                facility_specialty.quality_rating = rating_value
                logger.info(f"Aggiornato {facility.name}, Neurologia: {old_rating} -> {rating_value}")
                
                # Salviamo le modifiche
                session.commit()
                update_database_status(f"Corretta valutazione Neurologia per {facility.name}")
                return True
            else:
                logger.error(f"Relazione Neurologia non trovata per {facility.name}")
                return False

if __name__ == "__main__":
    # Ospedale di Gubbio - Gualdo Tadino
    facility_id = 1977
    csv_file = "./attached_assets/medical_facilities_full_ratings.csv"
    facility_csv_name = "Ospedale di Gubbio"
    
    print(f"Correzione valutazione Neurologia per Ospedale di Gubbio - Gualdo Tadino (ID: {facility_id})...")
    csv_data = load_csv_data(csv_file, facility_csv_name)
    
    if csv_data:
        print(f"Dati CSV trovati:")
        for key, value in csv_data.items():
            if key not in ['Name of the facility', 'City']:
                print(f"  {key}: {value}")
        
        # Aggiorniamo direttamente il valore di Neurologia
        neurologia_value = csv_data.get('Neurologia')
        if neurologia_value is not None and isinstance(neurologia_value, (int, float)):
            success = fix_neurologia_value(facility_id, neurologia_value)
            if success:
                print(f"\nAggiornamento diretto di Neurologia a {neurologia_value} completato.")
            else:
                print("\nErrore nell'aggiornamento di Neurologia.")
        else:
            print("\nValore di Neurologia non valido nei dati CSV.")
    else:
        print(f"Errore: struttura '{facility_csv_name}' non trovata nel CSV")