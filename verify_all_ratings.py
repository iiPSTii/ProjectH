"""
Script per verificare che tutte le valutazioni nel database corrispondano a quelle nel file CSV.

Questo script controlla ogni struttura nel database e verifica che le valutazioni delle specialità
corrispondano esattamente a quelle nel file CSV originale.
"""

import csv
import logging
import sys
from collections import defaultdict
from sqlalchemy import text
from sqlalchemy.orm import Session
from app import app, db
from models import MedicalFacility, Specialty, FacilitySpecialty

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
            SELECT s.name, fs.quality_rating
            FROM facility_specialty fs
            JOIN specialties s ON fs.specialty_id = s.id
            WHERE fs.facility_id = :facility_id
            """
            result = session.execute(text(query), {"facility_id": facility_id})
            
            # Creo un dizionario con nome specialità -> rating
            specialties = {}
            for row in result:
                specialty_name, rating = row
                specialties[specialty_name] = rating
                
            return specialties

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
    
    # Caso 2: Corrispondenza parziale
    for csv_name in csv_data.keys():
        # Se il nome nel DB è contenuto nel nome CSV
        if db_facility_name in csv_name:
            return csv_name
        # Se il nome CSV è contenuto nel nome DB
        if csv_name in db_facility_name:
            return csv_name
    
    # Caso 3: Corrispondenza con regole specifiche
    # Esempi di regole specifiche per strutture con nomi particolari
    special_mappings = {
        'IDI - Istituto Dermopatico dell\'Immacolata': 'IDI',
        'Ospedale San Paolo - Savona': 'Ospedale San Paolo',
        'Ospedale San Martino - Belluno': 'Ospedale San Martino',
        'AORN Dei Colli - Monaldi': 'AORN Dei Colli',
        'AORN Dei Colli - Cotugno': 'AORN Dei Colli',
        'Ospedale Morgagni - Pierantoni': 'Ospedale Morgagni',
        'Ospedale di Gubbio - Gualdo Tadino': 'Ospedale di Gubbio'
    }
    
    if db_facility_name in special_mappings and special_mappings[db_facility_name] in csv_data:
        return special_mappings[db_facility_name]
    
    # Non trovata
    return None

def verify_facility_ratings(facility, csv_data):
    """
    Verifica che le valutazioni di una struttura nel database corrispondano a quelle nel CSV
    
    Args:
        facility: L'oggetto MedicalFacility
        csv_data: Dati CSV
        
    Returns:
        dict: Dizionario con statistiche e discrepanze
    """
    stats = {
        'total': 0,
        'match': 0,
        'mismatch': 0,
        'missing_db': 0,
        'missing_csv': 0,
        'discrepancies': []
    }
    
    # Trovo il nome della struttura nel CSV
    csv_facility_name = find_csv_facility_name(facility.name, csv_data, facility.city)
    if not csv_facility_name:
        logger.warning(f"Struttura '{facility.name}' non trovata nel CSV")
        stats['discrepancies'].append({
            'facility': facility.name,
            'error': 'Struttura non trovata nel CSV'
        })
        return stats
    
    # Ottengo le specialità dal database
    db_specialties = get_facility_specialties(facility.id)
    
    # Ottengo i dati dal CSV
    csv_facility_data = csv_data[csv_facility_name]
    
    # Per ogni specialità nel CSV
    for csv_column, db_name in SPECIALTY_MAPPING.items():
        stats['total'] += 1
        
        # Ottengo il valore dal CSV
        csv_value = csv_facility_data.get(csv_column)
        
        # Ottengo il valore dal database
        db_value = db_specialties.get(db_name)
        
        # Se entrambi i valori sono None, li considero uguali
        if csv_value is None and db_value is None:
            stats['match'] += 1
            continue
        
        # Se il valore nel CSV è None ma nel DB esiste
        if csv_value is None and db_value is not None:
            stats['missing_csv'] += 1
            stats['discrepancies'].append({
                'facility': facility.name,
                'specialty': db_name,
                'csv_value': None,
                'db_value': db_value
            })
            continue
        
        # Se il valore nel DB è None ma nel CSV esiste
        if csv_value is not None and db_value is None:
            stats['missing_db'] += 1
            stats['discrepancies'].append({
                'facility': facility.name,
                'specialty': db_name,
                'csv_value': csv_value,
                'db_value': None
            })
            continue
        
        # Se i valori sono diversi
        if abs(float(csv_value) - float(db_value)) > 0.01:  # Tolleranza per errori di arrotondamento
            stats['mismatch'] += 1
            stats['discrepancies'].append({
                'facility': facility.name,
                'specialty': db_name,
                'csv_value': csv_value,
                'db_value': db_value
            })
        else:
            stats['match'] += 1
    
    return stats

def verify_all_facilities(csv_file):
    """
    Verifica le valutazioni per tutte le strutture
    
    Args:
        csv_file: Il percorso del file CSV
        
    Returns:
        dict: Statistiche complessive
    """
    # Carico i dati dal CSV
    csv_data = load_csv_data(csv_file)
    
    # Ottengo tutte le strutture dal database
    facilities = get_all_facilities()
    logger.info(f"Trovate {len(facilities)} strutture nel database")
    
    # Statistiche complessive
    total_stats = {
        'total_facilities': len(facilities),
        'verified_facilities': 0,
        'facilities_with_discrepancies': 0,
        'total_ratings': 0,
        'matching_ratings': 0,
        'mismatched_ratings': 0,
        'missing_in_db': 0,
        'missing_in_csv': 0,
        'discrepancies_by_facility': defaultdict(list)
    }
    
    # Per ogni struttura
    for facility in facilities:
        # Verifico le valutazioni
        stats = verify_facility_ratings(facility, csv_data)
        
        # Aggiorno le statistiche complessive
        total_stats['verified_facilities'] += 1
        total_stats['total_ratings'] += stats['total']
        total_stats['matching_ratings'] += stats['match']
        total_stats['mismatched_ratings'] += stats['mismatch']
        total_stats['missing_in_db'] += stats['missing_db']
        total_stats['missing_in_csv'] += stats['missing_csv']
        
        # Se ci sono discrepanze
        if stats['mismatch'] > 0 or stats['missing_db'] > 0 or stats['missing_csv'] > 0:
            total_stats['facilities_with_discrepancies'] += 1
            for discrepancy in stats['discrepancies']:
                total_stats['discrepancies_by_facility'][facility.name].append(discrepancy)
    
    return total_stats

def print_discrepancies(stats):
    """
    Stampa le discrepanze trovate durante la verifica
    
    Args:
        stats: Statistiche della verifica
    """
    print("\n\n========== DISCREPANZE TROVATE ==========")
    print(f"Strutture con discrepanze: {stats['facilities_with_discrepancies']} su {stats['verified_facilities']} verificate")
    
    # Stampa le discrepanze per struttura
    for facility_name, discrepancies in stats['discrepancies_by_facility'].items():
        print(f"\n--- {facility_name} ---")
        for discrepancy in discrepancies:
            if 'error' in discrepancy:
                print(f"ERRORE: {discrepancy['error']}")
            else:
                csv_value = discrepancy.get('csv_value', 'N/A')
                db_value = discrepancy.get('db_value', 'N/A')
                print(f"Specialità: {discrepancy['specialty']}, CSV: {csv_value}, DB: {db_value}")

def print_stats(stats):
    """
    Stampa le statistiche della verifica
    
    Args:
        stats: Statistiche della verifica
    """
    print("\n========== STATISTICHE ==========")
    print(f"Strutture verificate: {stats['verified_facilities']} su {stats['total_facilities']}")
    print(f"Valutazioni totali: {stats['total_ratings']}")
    print(f"Valutazioni corrispondenti: {stats['matching_ratings']} ({stats['matching_ratings']/stats['total_ratings']*100:.2f}%)")
    print(f"Valutazioni non corrispondenti: {stats['mismatched_ratings']} ({stats['mismatched_ratings']/stats['total_ratings']*100:.2f}%)")
    print(f"Valutazioni mancanti nel database: {stats['missing_in_db']} ({stats['missing_in_db']/stats['total_ratings']*100:.2f}%)")
    print(f"Valutazioni mancanti nel CSV: {stats['missing_in_csv']} ({stats['missing_in_csv']/stats['total_ratings']*100:.2f}%)")

if __name__ == "__main__":
    csv_file = "./attached_assets/medical_facilities_full_ratings.csv"
    print(f"Verifica delle valutazioni per tutte le strutture dal file {csv_file}...")
    
    # Eseguo la verifica
    stats = verify_all_facilities(csv_file)
    
    # Stampo le statistiche
    print_stats(stats)
    
    # Stampo le discrepanze
    print_discrepancies(stats)
    
    # Suggerisco come aggiornare
    if stats['facilities_with_discrepancies'] > 0:
        print("\n========== SUGGERIMENTO ==========")
        print("Sono state trovate discrepanze tra il CSV e il database.")
        print("Per correggere tutte le discrepanze, esegui questo comando:")
        print("python fix_all_discrepancies.py")
    else:
        print("\nTutte le valutazioni sono corrette!")