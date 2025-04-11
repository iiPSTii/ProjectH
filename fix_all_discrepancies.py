"""
Script per correggere tutte le discrepanze tra i dati nel CSV e quelli nel database.

Questo script confronta le valutazioni nel database con quelle nel file CSV originale
e corregge automaticamente le discrepanze trovate.
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

# Mappatura delle strutture con nomi diversi tra CSV e DB
FACILITY_MAPPINGS = {
    'IDI - Istituto Dermopatico dell\'Immacolata': 'IDI',
    'Ospedale San Paolo': 'Ospedale San Paolo',
    'Ospedale San Martino': 'Ospedale San Martino',
    'AORN Dei Colli - Monaldi': 'AORN Dei Colli',
    'AORN Dei Colli - Cotugno': 'AORN Dei Colli',
    'Ospedale Morgagni - Pierantoni': 'Ospedale Morgagni',
    'Ospedale di Gubbio - Gualdo Tadino': 'Ospedale di Gubbio',
    'Clinica Villa Bianca': 'Clinica Villa Bianca',
    'Ospedale "SS. Annunziata"': 'Ospedale "SS. Annunziata"',
    'Ospedale di Cavalese': 'Ospedale di Cavalese',
    'Centro Medico Fiorentino': 'Centro Medico Fiorentino',
    'Azienda Ospedaliero Universitaria Careggi': 'Azienda Ospedaliero Universitaria Careggi',
    'Istituto Tumori "Giovanni Paolo II"': 'Istituto Tumori "Giovanni Paolo II"',
    'Ospedale "Casa Sollievo della Sofferenza"': 'Ospedale "Casa Sollievo della Sofferenza"',
    'Villa Bianca': 'Villa Bianca'
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

def get_all_facilities():
    """
    Ottiene tutte le strutture dal database
    
    Returns:
        list: Lista di oggetti MedicalFacility
    """
    with app.app_context():
        with Session(db.engine) as session:
            facilities = session.query(MedicalFacility).all()
            return facilities

def get_facility_specialties(facility_id):
    """
    Ottiene tutte le specialità e i relativi rating per una struttura
    
    Args:
        facility_id: ID della struttura
        
    Returns:
        dict: Dizionario con nome specialità -> rating
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

def find_csv_facility_name(db_facility_name, csv_data, city=None):
    """
    Trova il nome della struttura nel CSV che corrisponde al nome nel database
    
    Args:
        db_facility_name: Nome della struttura nel database
        csv_data: Dati CSV
        city: Città della struttura (opzionale)
        
    Returns:
        str: Nome della struttura nel CSV, o None se non trovata
    """
    # Caso 1: Corrispondenza esatta
    if db_facility_name in csv_data:
        return db_facility_name
    
    # Caso 2: Mappatura specifica
    if db_facility_name in FACILITY_MAPPINGS and FACILITY_MAPPINGS[db_facility_name] in csv_data:
        return FACILITY_MAPPINGS[db_facility_name]
    
    # Caso 3: Corrispondenza parziale
    for csv_name in csv_data.keys():
        # Se il nome nel DB è contenuto nel nome CSV
        if db_facility_name in csv_name:
            return csv_name
        # Se il nome CSV è contenuto nel nome DB
        if csv_name in db_facility_name:
            return csv_name
    
    # Non trovata
    return None

def fix_facility_ratings(facility, csv_data, dry_run=False):
    """
    Corregge le valutazioni di una struttura in base ai dati del CSV
    
    Args:
        facility: L'oggetto MedicalFacility
        csv_data: Dati CSV
        dry_run: Se True, non apporta modifiche ma stampa solo le discrepanze
        
    Returns:
        dict: Statistiche sulle correzioni
    """
    stats = {
        'created': 0,
        'updated': 0,
        'skipped': 0,
        'discrepancies': []
    }
    
    # Trovo il nome della struttura nel CSV
    csv_facility_name = find_csv_facility_name(facility.name, csv_data, facility.city)
    if not csv_facility_name:
        logger.warning(f"Struttura '{facility.name}' non trovata nel CSV")
        return stats
    
    # Ottengo le specialità dal database
    db_specialties = get_facility_specialties(facility.id)
    
    # Ottengo i dati dal CSV
    csv_facility_data = csv_data[csv_facility_name]
    
    with app.app_context():
        with Session(db.engine) as session:
            # Per ogni specialità nel CSV
            for csv_column, db_name in SPECIALTY_MAPPING.items():
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
                    
                    # Se è un dry run, aggiungo solo la discrepanza
                    if dry_run:
                        stats['discrepancies'].append({
                            'facility': facility.name,
                            'specialty': db_name,
                            'csv_value': csv_value,
                            'db_value': db_value
                        })
                        continue
                    
                    # Altrimenti, aggiorno o creo la valutazione
                    if db_value is not None:
                        # Aggiorno
                        facility_specialty = session.query(FacilitySpecialty).filter_by(
                            facility_id=facility.id,
                            specialty_id=specialty_id
                        ).first()
                        
                        facility_specialty.quality_rating = csv_value
                        logger.info(f"Aggiornato {facility.name}, {db_name}: {db_value} -> {csv_value}")
                        stats['updated'] += 1
                    else:
                        # Creo
                        new_rating = FacilitySpecialty(
                            facility_id=facility.id,
                            specialty_id=specialty_id,
                            quality_rating=csv_value
                        )
                        session.add(new_rating)
                        logger.info(f"Creato {facility.name}, {db_name}: {csv_value}")
                        stats['created'] += 1
                else:
                    # Il valore è già corretto
                    stats['skipped'] += 1
            
            # Se non è un dry run, salvo le modifiche
            if not dry_run and (stats['created'] > 0 or stats['updated'] > 0):
                session.commit()
                logger.info(f"Salvate {stats['created'] + stats['updated']} modifiche per {facility.name}")
    
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

def fix_all_facilities(csv_file, dry_run=False, backup=True):
    """
    Corregge le valutazioni per tutte le strutture
    
    Args:
        csv_file: Il percorso del file CSV
        dry_run: Se True, non apporta modifiche ma stampa solo le discrepanze
        backup: Se True, crea un backup del database prima di apportare modifiche
        
    Returns:
        dict: Statistiche complessive
    """
    # Carico i dati dal CSV
    csv_data = load_csv_data(csv_file)
    
    # Ottengo tutte le strutture dal database
    facilities = get_all_facilities()
    logger.info(f"Trovate {len(facilities)} strutture nel database")
    
    # Creo un backup del database se richiesto
    if backup and not dry_run:
        backup_file = backup_database()
        logger.info(f"Backup creato: {backup_file}")
    
    # Statistiche complessive
    total_stats = {
        'total_facilities': len(facilities),
        'processed_facilities': 0,
        'facilities_with_changes': 0,
        'total_created': 0,
        'total_updated': 0,
        'total_skipped': 0,
        'discrepancies': []
    }
    
    # Per ogni struttura
    for i, facility in enumerate(facilities):
        # Fisso la struttura
        stats = fix_facility_ratings(facility, csv_data, dry_run)
        
        # Aggiorno le statistiche complessive
        total_stats['processed_facilities'] += 1
        total_stats['total_created'] += stats['created']
        total_stats['total_updated'] += stats['updated']
        total_stats['total_skipped'] += stats['skipped']
        
        # Se ci sono correzioni
        if stats['created'] > 0 or stats['updated'] > 0 or len(stats['discrepancies']) > 0:
            total_stats['facilities_with_changes'] += 1
            total_stats['discrepancies'].extend(stats['discrepancies'])
        
        # Stampo l'avanzamento ogni 10 strutture
        if (i + 1) % 10 == 0:
            print(f"Processate {i + 1}/{len(facilities)} strutture...")
    
    # Aggiorno lo stato del database
    if not dry_run and (total_stats['total_created'] > 0 or total_stats['total_updated'] > 0):
        update_database_status(f"Corrette {total_stats['total_created'] + total_stats['total_updated']} valutazioni per {total_stats['facilities_with_changes']} strutture")
    
    return total_stats

def print_discrepancies(stats):
    """
    Stampa le discrepanze trovate
    
    Args:
        stats: Statistiche delle correzioni
    """
    if len(stats['discrepancies']) > 0:
        print("\n========== DISCREPANZE TROVATE ==========")
        print(f"Trovate {len(stats['discrepancies'])} discrepanze in {stats['facilities_with_changes']} strutture")
        
        # Raggruppo le discrepanze per struttura
        discrepancies_by_facility = {}
        for discrepancy in stats['discrepancies']:
            facility_name = discrepancy['facility']
            if facility_name not in discrepancies_by_facility:
                discrepancies_by_facility[facility_name] = []
            discrepancies_by_facility[facility_name].append(discrepancy)
        
        # Stampo le discrepanze
        for facility_name, facility_discrepancies in discrepancies_by_facility.items():
            print(f"\n--- {facility_name} ---")
            for discrepancy in facility_discrepancies:
                csv_value = discrepancy.get('csv_value', 'N/A')
                db_value = discrepancy.get('db_value', 'N/A')
                print(f"Specialità: {discrepancy['specialty']}, CSV: {csv_value}, DB: {db_value}")

def print_stats(stats, dry_run=False):
    """
    Stampa le statistiche delle correzioni
    
    Args:
        stats: Statistiche delle correzioni
        dry_run: Se True, stampa le statistiche in modalità dry run
    """
    print("\n========== STATISTICHE ==========")
    print(f"Strutture processate: {stats['processed_facilities']} su {stats['total_facilities']}")
    
    if dry_run:
        print(f"Strutture con discrepanze: {stats['facilities_with_changes']}")
        print(f"Discrepanze totali: {len(stats['discrepancies'])}")
    else:
        print(f"Strutture con correzioni: {stats['facilities_with_changes']}")
        print(f"Valutazioni create: {stats['total_created']}")
        print(f"Valutazioni aggiornate: {stats['total_updated']}")
        print(f"Valutazioni saltate: {stats['total_skipped']}")

if __name__ == "__main__":
    csv_file = "./attached_assets/medical_facilities_full_ratings.csv"
    
    # Modalità automatica per esecuzione senza input utente
    print("Questo script correggerà tutte le discrepanze nelle valutazioni delle strutture.")
    print("Prima di procedere, verrà creato un backup del database.")
    
    # Eseguo prima in dry run per mostrare le discrepanze
    print("Esecuzione in dry run per mostrare le discrepanze...")
    dry_run_stats = fix_all_facilities(csv_file, dry_run=True, backup=False)
    
    # Stampo le statistiche e le discrepanze
    print_stats(dry_run_stats, dry_run=True)
    print_discrepancies(dry_run_stats)
    
    # Procedo automaticamente alla correzione
    if len(dry_run_stats['discrepancies']) > 0:
        print(f"\nSono state trovate {len(dry_run_stats['discrepancies'])} discrepanze.")
        print("Procedo con la correzione...")
        
        # Eseguo la correzione
        stats = fix_all_facilities(csv_file)
        
        # Stampo le statistiche
        print_stats(stats)
        
        print("\nCorrezione completata!")
    else:
        print("\nTutte le valutazioni sono già corrette!")