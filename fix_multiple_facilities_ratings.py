"""
Script per aggiungere/aggiornare le valutazioni per più strutture specifiche

Questo script estrae le valutazioni per le strutture specificate dal file CSV
e le aggiorna nel database.
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

# Elenco delle strutture da aggiornare e loro mappatura con i nomi nel CSV
# Formato: 'nome nel database': 'nome nel CSV'
FACILITIES_MAPPING = {
    "Ospedale di Dolo": "Ospedale di Dolo",
    "Ospedale San Paolo": "Ospedale San Paolo", # (Savona)
    "Ospedale San Martino": "Ospedale San Martino", # (Belluno)
    "Ospedale \"Casa Sollievo della Sofferenza\"": "Ospedale \"Casa Sollievo della Sofferenza\"",
    "Villa Bianca": "Villa Bianca", # (Arco)
    "AORN Dei Colli - Monaldi": "AORN Dei Colli", # (Monaldi)
    "AORN Dei Colli - Cotugno": "AORN Dei Colli", # (Cotugno)
    "Azienda Ospedaliero Universitaria Careggi": "Azienda Ospedaliero Universitaria Careggi",
    "Clinica Villa Bianca": "Clinica Villa Bianca", # (Lecce)
    "Ospedale \"SS. Annunziata\"": "Ospedale \"SS. Annunziata\"", # (Taranto)
    "Ospedale di Cavalese": "Ospedale di Cavalese",
    "Centro Medico Fiorentino": "Centro Medico Fiorentino",
    "Ospedale Morgagni - Pierantoni": "Ospedale Morgagni", # (Forlì)
    "Istituto Tumori \"Giovanni Paolo II\"": "Istituto Tumori \"Giovanni Paolo II\"", # (Bari)
    "Ospedale di Gubbio - Gualdo Tadino": "Ospedale di Gubbio" # (Gualdo Tadino - Gubbio)
}

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
            logger.debug(f"Caricati dati per {facility_name}: {row}")
    
    logger.info(f"Caricate {len(facilities_data)} strutture dal CSV")
    return facilities_data

def get_specialty_id_by_name(session, name):
    """Get specialty ID by name with fuzzy matching"""
    specialty = session.query(Specialty).filter(
        Specialty.name.ilike(f"%{name}%")
    ).first()
    
    if specialty:
        return specialty.id
    return None

def get_facility_by_name(session, name):
    """Get facility by name with fuzzy matching"""
    facility = session.query(MedicalFacility).filter(
        MedicalFacility.name.ilike(f"%{name}%")
    ).first()
    
    return facility

def get_all_facilities(session, names):
    """Get all facilities by name"""
    results = {}
    
    for db_name in names:
        facility = get_facility_by_name(session, db_name)
        if facility:
            results[db_name] = facility
        else:
            logger.warning(f"Struttura non trovata nel database: {db_name}")
    
    return results

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

def fix_facilities_ratings(csv_file):
    """
    Aggiorna le valutazioni per le strutture specificate
    
    Args:
        csv_file: Il percorso del file CSV
    """
    # Prima carichiamo tutti i dati dal CSV
    csv_data = load_csv_data(csv_file)
    logger.info(f"Caricati dati per {len(csv_data)} strutture dal CSV")
    
    # Creiamo un backup del database
    with app.app_context():
        backup_file = backup_database()
        logger.info(f"Backup creato: {backup_file}")
        
        with Session(db.engine) as session:
            # Otteniamo gli ID delle specialità
            specialty_ids = {}
            for _, spec_name in SPECIALTY_MAPPING.items():
                specialty_id = get_specialty_id_by_name(session, spec_name)
                if specialty_id:
                    specialty_ids[spec_name] = specialty_id
                else:
                    logger.warning(f"Specialità {spec_name} non trovata nel database")
            
            logger.info(f"Trovate {len(specialty_ids)} specialità nel database")
            
            # Otteniamo le strutture dal database
            facilities = get_all_facilities(session, FACILITIES_MAPPING.keys())
            logger.info(f"Trovate {len(facilities)} strutture nel database")
            
            # Statistiche totali
            total_stats = {'updated': 0, 'created': 0, 'skipped': 0, 'facility_not_found': 0}
            
            # Per ogni struttura da aggiornare
            for db_name, csv_name in FACILITIES_MAPPING.items():
                if db_name not in facilities:
                    total_stats['facility_not_found'] += 1
                    continue
                    
                if csv_name not in csv_data:
                    logger.warning(f"Struttura {csv_name} non trovata nel CSV")
                    total_stats['facility_not_found'] += 1
                    continue
                
                facility = facilities[db_name]
                
                logger.info(f"Aggiornamento valutazioni per {db_name} (ID: {facility.id})")
                stats = update_facility_ratings(facility, csv_data[csv_name], session, specialty_ids)
                
                # Aggiorniamo le statistiche totali
                for key in stats:
                    total_stats[key] += stats[key]
            
            # Salviamo le modifiche
            session.commit()
            
            # Aggiorniamo lo stato del database
            update_database_status(f"Aggiornate valutazioni per {len(facilities)} strutture")
            
            return total_stats

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

def print_stats(stats):
    """Stampa le statistiche dell'aggiornamento"""
    print("\nAggiornamento completato:")
    print(f"  Valutazioni aggiornate: {stats['updated']}")
    print(f"  Valutazioni create:     {stats['created']}")
    print(f"  Valutazioni saltate:    {stats['skipped']}")
    print(f"  Strutture non trovate:  {stats['facility_not_found']}")

if __name__ == "__main__":
    csv_file = "./attached_assets/medical_facilities_full_ratings.csv"
    print(f"Aggiornamento valutazioni per strutture selezionate da {csv_file}...")
    
    # Esegui l'aggiornamento
    stats = fix_facilities_ratings(csv_file)
    
    # Stampa le statistiche
    print_stats(stats)
    
    print("\nAggiornamento completato. Un backup del database è stato creato.")