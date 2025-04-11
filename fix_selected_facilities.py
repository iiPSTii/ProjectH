"""
Script per correggere le valutazioni solo per le strutture selezionate.

Questo script corregge le valutazioni solo per le strutture che sono state
identificate con discrepanze nel CSV.
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

# Lista di strutture da correggere
FACILITIES_TO_FIX = [
    'IDI - Istituto Dermopatico dell\'Immacolata',
    'Ospedale San Carlo Borromeo',
    'Ospedale Umberto Parini',
    'Ospedale San Giovanni di Dio'
]

# Le colonne nel CSV sono in questo ordine:
CSV_SPECIALTIES = [
    'Cardiologia',
    'Ortopedia',
    'Oncologia',
    'Neurologia',
    'Urologia',
    'Chirurgia generale',  # Nel DB è 'Chirurgia Generale'
    'Pediatria',
    'Ginecologia'
]

# Mappatura dei nomi delle colonne CSV ai nomi delle specialità nel database
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
            for specialty in CSV_SPECIALTIES:
                if specialty in row and row[specialty].strip():
                    try:
                        row[specialty] = float(row[specialty])
                    except ValueError:
                        logger.warning(f"Valore non valido per {facility_name}, {specialty}: {row[specialty]}")
                        row[specialty] = None
            
            facilities_data[facility_name] = row
    
    logger.info(f"Caricate {len(facilities_data)} strutture dal CSV")
    return facilities_data

def get_facility_by_name(name):
    """
    Ottiene una struttura dal database tramite il nome
    
    Args:
        name: Nome della struttura
        
    Returns:
        MedicalFacility: Oggetto struttura, o None se non trovata
    """
    with app.app_context():
        with Session(db.engine) as session:
            facility = session.query(MedicalFacility).filter(
                MedicalFacility.name == name
            ).first()
            return facility

def get_facility_specialties(facility_id):
    """
    Ottiene tutte le specialità e i relativi rating per una struttura
    
    Args:
        facility_id: ID della struttura
        
    Returns:
        dict: Dizionario con nome specialità -> (rating, id)
    """
    with app.app_context():
        with Session(db.engine) as session:
            # Query per ottenere le specialità con rating
            query = """
            SELECT s.name, fs.quality_rating, s.id
            FROM facility_specialty fs
            JOIN specialties s ON fs.specialty_id = s.id
            WHERE fs.facility_id = :facility_id
            """
            result = session.execute(text(query), {"facility_id": facility_id})
            
            # Creo un dizionario con nome specialità -> (rating, id)
            specialties = {}
            for row in result:
                specialty_name, rating, specialty_id = row
                specialties[specialty_name] = (rating, specialty_id)
                
            return specialties

def get_specialty_id_by_name(session, name):
    """
    Ottiene l'ID di una specialità dal nome
    
    Args:
        session: Sessione del database
        name: Nome della specialità
        
    Returns:
        int: ID della specialità, o None se non trovata
    """
    specialty = session.query(Specialty).filter(
        Specialty.name == name
    ).first()
    
    if specialty:
        return specialty.id
    
    return None

def find_csv_facility_name(db_facility_name, csv_data):
    """
    Trova il nome della struttura nel CSV che corrisponde al nome nel database
    
    Args:
        db_facility_name: Nome della struttura nel database
        csv_data: Dati CSV
        
    Returns:
        str: Nome della struttura nel CSV, o None se non trovata
    """
    # Caso 1: Corrispondenza esatta
    if db_facility_name in csv_data:
        return db_facility_name
    
    # Caso 2: Corrispondenza parziale
    for csv_name in csv_data.keys():
        # Se il nome nel DB è contenuto nel nome CSV
        if db_facility_name in csv_name:
            return csv_name
        # Se il nome CSV è contenuto nel nome DB
        if csv_name in db_facility_name:
            return csv_name
    
    # Caso 3: Corrispondenza con regole specifiche
    special_mappings = {
        'IDI - Istituto Dermopatico dell\'Immacolata': 'IDI',
        'Ospedale di Gubbio - Gualdo Tadino': 'Ospedale di Gubbio',
        'Ospedale San Giovanni di Dio - Gorizia': 'Ospedale San Giovanni di Dio',
        'Ospedale San Giovanni di Dio - Crotone': 'Ospedale San Giovanni di Dio',
        'Ospedale San Paolo - Savona': 'Ospedale San Paolo',
        'Ospedale San Paolo - Milano': 'Ospedale San Paolo'
    }
    
    if db_facility_name in special_mappings and special_mappings[db_facility_name] in csv_data:
        return special_mappings[db_facility_name]
    
    # Non trovata
    return None

def update_facility_ratings(facility, csv_data):
    """
    Aggiorna le valutazioni di una struttura in base ai dati del CSV
    
    Args:
        facility: L'oggetto MedicalFacility
        csv_data: Dati CSV
        
    Returns:
        dict: Statistiche sugli aggiornamenti
    """
    stats = {
        'total': 0,
        'updated': 0,
        'skipped': 0,
        'not_found': 0
    }
    
    # Trovo il nome della struttura nel CSV
    csv_facility_name = find_csv_facility_name(facility.name, csv_data)
    if not csv_facility_name:
        logger.warning(f"Struttura '{facility.name}' non trovata nel CSV")
        stats['not_found'] += 1
        return stats
    
    # Ottengo le specialità dal database
    db_specialties = get_facility_specialties(facility.id)
    
    # Ottengo i dati dal CSV
    csv_facility_data = csv_data[csv_facility_name]
    
    # Per ogni specialità nel CSV
    with app.app_context():
        with Session(db.engine) as session:
            updates = []
            
            for csv_column, db_name in SPECIALTY_MAPPING.items():
                stats['total'] += 1
                
                # Ottengo il valore dal CSV
                csv_value = csv_facility_data.get(csv_column)
                if csv_value is None:
                    stats['skipped'] += 1
                    continue
                
                # Ottengo il valore dal database
                db_info = db_specialties.get(db_name)
                if db_info:
                    db_value, specialty_id = db_info
                else:
                    db_value, specialty_id = None, None
                
                # Se il valore nel DB è None o diverso dal CSV
                if db_value is None or abs(float(csv_value) - float(db_value)) > 0.01:
                    # Ottengo l'ID della specialità
                    if specialty_id is None:
                        specialty_id = get_specialty_id_by_name(session, db_name)
                        if specialty_id is None:
                            logger.warning(f"Specialità '{db_name}' non trovata nel database")
                            stats['skipped'] += 1
                            continue
                    
                    # Aggiorno o creo la valutazione
                    if db_value is not None:
                        # Aggiorno
                        facility_specialty = session.query(FacilitySpecialty).filter_by(
                            facility_id=facility.id,
                            specialty_id=specialty_id
                        ).first()
                        
                        if facility_specialty:
                            updates.append(f"{db_name}: {db_value} -> {csv_value}")
                            facility_specialty.quality_rating = csv_value
                            stats['updated'] += 1
                        else:
                            logger.warning(f"FacilitySpecialty non trovata per {facility.name}, {db_name}")
                            stats['skipped'] += 1
                    else:
                        # Creo
                        new_rating = FacilitySpecialty(
                            facility_id=facility.id,
                            specialty_id=specialty_id,
                            quality_rating=csv_value
                        )
                        updates.append(f"{db_name}: None -> {csv_value}")
                        session.add(new_rating)
                        stats['updated'] += 1
                else:
                    # Il valore è già corretto
                    stats['skipped'] += 1
            
            # Salvo le modifiche
            if stats['updated'] > 0:
                session.commit()
                logger.info(f"Aggiornata struttura {facility.name} ({stats['updated']} modifiche)")
                for update in updates:
                    logger.info(f"  {update}")
    
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

def fix_selected_facilities(csv_file, facilities_to_fix):
    """
    Corregge le valutazioni per le strutture selezionate
    
    Args:
        csv_file: Il percorso del file CSV
        facilities_to_fix: Lista di nomi di strutture da correggere
        
    Returns:
        dict: Statistiche sugli aggiornamenti
    """
    # Carico i dati dal CSV
    csv_data = load_csv_data(csv_file)
    
    # Creo un backup del database
    with app.app_context():
        backup_file = backup_database()
        logger.info(f"Backup creato: {backup_file}")
    
    # Statistiche complessive
    total_stats = {
        'total_facilities': len(facilities_to_fix),
        'processed_facilities': 0,
        'facilities_with_updates': 0,
        'facilities_not_found': 0,
        'total_updated': 0,
        'total_skipped': 0,
        'updates_by_facility': {}
    }
    
    # Per ogni struttura
    for facility_name in facilities_to_fix:
        # Ottengo la struttura dal database
        facility = get_facility_by_name(facility_name)
        if not facility:
            logger.warning(f"Struttura '{facility_name}' non trovata nel database")
            total_stats['facilities_not_found'] += 1
            continue
        
        # Aggiorno la struttura
        stats = update_facility_ratings(facility, csv_data)
        
        # Aggiorno le statistiche complessive
        total_stats['processed_facilities'] += 1
        total_stats['total_updated'] += stats['updated']
        total_stats['total_skipped'] += stats['skipped']
        
        # Se ci sono aggiornamenti
        if stats['updated'] > 0:
            total_stats['facilities_with_updates'] += 1
            total_stats['updates_by_facility'][facility.name] = stats['updated']
    
    # Aggiorno lo stato del database
    if total_stats['total_updated'] > 0:
        update_database_status(f"Aggiornate {total_stats['total_updated']} valutazioni per {total_stats['facilities_with_updates']} strutture")
    
    return total_stats

def print_stats(stats):
    """
    Stampa le statistiche degli aggiornamenti
    
    Args:
        stats: Statistiche degli aggiornamenti
    """
    print("\n========== STATISTICHE ==========")
    print(f"Strutture processate: {stats['processed_facilities']} su {stats['total_facilities']}")
    print(f"Strutture aggiornate: {stats['facilities_with_updates']}")
    print(f"Strutture non trovate: {stats['facilities_not_found']}")
    print(f"Valutazioni aggiornate: {stats['total_updated']}")
    print(f"Valutazioni saltate: {stats['total_skipped']}")
    
    if stats['facilities_with_updates'] > 0:
        print("\n--- Aggiornamenti per struttura ---")
        for facility_name, num_updates in stats['updates_by_facility'].items():
            print(f"{facility_name}: {num_updates} aggiornamenti")

if __name__ == "__main__":
    csv_file = "./attached_assets/medical_facilities_full_ratings.csv"
    print(f"Correzione delle valutazioni per strutture selezionate dal file {csv_file}...")
    
    # Eseguo la correzione
    stats = fix_selected_facilities(csv_file, FACILITIES_TO_FIX)
    
    # Stampo le statistiche
    print_stats(stats)
    
    if stats['total_updated'] > 0:
        print("\nCorrezione completata!")
    else:
        print("\nNessuna correzione necessaria.")