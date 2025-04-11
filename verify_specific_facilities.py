"""
Script per verificare specifiche strutture problematiche nel database.

Questo script verifica che le valutazioni di strutture specifiche nel database
corrispondano a quelle nel file CSV originale.
"""

import csv
import logging
from sqlalchemy import text
from sqlalchemy.orm import Session
from app import app, db
from models import MedicalFacility, Specialty, FacilitySpecialty

# Setup logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Lista di nomi delle strutture da verificare
FACILITIES_TO_CHECK = [
    'Ospedale di Gubbio - Gualdo Tadino',
    'IDI - Istituto Dermopatico dell\'Immacolata',
    'Ospedale San Carlo Borromeo',
    'Ospedale Umberto Parini',
    'Ospedale San Giovanni di Dio'
]

# Specialità da verificare per ogni struttura
SPECIALTIES_TO_CHECK = [
    'Cardiologia',
    'Ortopedia',
    'Oncologia',
    'Neurologia',
    'Urologia',
    'Chirurgia Generale',
    'Pediatria',
    'Ginecologia'
]

def get_db_ratings(facility_name, city=None):
    """
    Ottiene i rating dal database per una struttura
    
    Args:
        facility_name: Nome della struttura
        city: Città della struttura (opzionale)
        
    Returns:
        dict: Dizionario con nome specialità -> rating
    """
    with app.app_context():
        with Session(db.engine) as session:
            # Query per trovare l'ID della struttura
            query = session.query(MedicalFacility).filter(
                MedicalFacility.name == facility_name
            )
            
            if city:
                query = query.filter(MedicalFacility.city == city)
                
            facility = query.first()
            
            if not facility:
                logger.error(f"Struttura '{facility_name}' non trovata nel database")
                return None
            
            # Query per ottenere le specialità e i rating
            sql_query = """
            SELECT s.name, fs.quality_rating
            FROM facility_specialty fs
            JOIN specialties s ON fs.specialty_id = s.id
            WHERE fs.facility_id = :facility_id
            """
            result = session.execute(text(sql_query), {"facility_id": facility.id})
            
            ratings = {}
            for row in result:
                specialty_name, rating = row
                ratings[specialty_name] = rating
                
            return facility.id, ratings

def get_csv_ratings(csv_file, facility_name, city=None):
    """
    Ottiene i rating dal CSV per una struttura
    
    Args:
        csv_file: Percorso del file CSV
        facility_name: Nome della struttura
        city: Città della struttura (opzionale)
        
    Returns:
        dict: Dizionario con nome specialità -> rating
    """
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Se è specificata la città, verifica anche quella
            if row['Name of the facility'] == facility_name and (city is None or row['City'] == city):
                # Mappatura delle colonne CSV ai nomi delle specialità
                specialty_mapping = {
                    'Cardiologia': 'Cardiologia',
                    'Ortopedia': 'Ortopedia',
                    'Oncologia': 'Oncologia',
                    'Neurologia': 'Neurologia',
                    'Urologia': 'Urologia',
                    'Chirurgia generale': 'Chirurgia Generale',
                    'Pediatria': 'Pediatria',
                    'Ginecologia': 'Ginecologia'
                }
                
                ratings = {}
                for csv_name, db_name in specialty_mapping.items():
                    if csv_name in row and row[csv_name].strip():
                        try:
                            ratings[db_name] = float(row[csv_name])
                        except ValueError:
                            logger.warning(f"Valore non valido per {facility_name}, {csv_name}: {row[csv_name]}")
                
                return ratings
    
    # Gestione dei casi speciali
    if facility_name == 'Ospedale di Gubbio - Gualdo Tadino':
        return get_csv_ratings(csv_file, 'Ospedale di Gubbio')
    elif facility_name == 'IDI - Istituto Dermopatico dell\'Immacolata':
        return get_csv_ratings(csv_file, 'IDI')
    
    logger.error(f"Struttura '{facility_name}' non trovata nel CSV")
    return None

def compare_ratings(facility_name, db_ratings, csv_ratings):
    """
    Confronta i rating tra database e CSV
    
    Args:
        facility_name: Nome della struttura
        db_ratings: Rating dal database
        csv_ratings: Rating dal CSV
        
    Returns:
        list: Lista di discrepanze
    """
    discrepancies = []
    
    for specialty in SPECIALTIES_TO_CHECK:
        db_value = db_ratings.get(specialty)
        csv_value = csv_ratings.get(specialty)
        
        # Se entrambi i valori sono None, sono considerati uguali
        if db_value is None and csv_value is None:
            continue
        
        # Se uno dei valori è None ma l'altro no
        if db_value is None and csv_value is not None:
            discrepancies.append({
                'facility': facility_name,
                'specialty': specialty,
                'db_value': None,
                'csv_value': csv_value,
                'note': 'Mancante nel database'
            })
            continue
            
        if csv_value is None and db_value is not None:
            discrepancies.append({
                'facility': facility_name,
                'specialty': specialty,
                'db_value': db_value,
                'csv_value': None,
                'note': 'Mancante nel CSV'
            })
            continue
        
        # Se i valori sono diversi (con una tolleranza per errori di arrotondamento)
        if abs(db_value - csv_value) > 0.01:
            discrepancies.append({
                'facility': facility_name,
                'specialty': specialty,
                'db_value': db_value,
                'csv_value': csv_value,
                'note': 'Valori diversi'
            })
    
    return discrepancies

def check_specific_facilities(csv_file, facility_names):
    """
    Verifica specifiche strutture
    
    Args:
        csv_file: Percorso del file CSV
        facility_names: Lista di nomi delle strutture da verificare
        
    Returns:
        dict: Risultati della verifica
    """
    results = {
        'total_checked': 0,
        'with_discrepancies': 0,
        'discrepancies': [],
        'not_found_in_db': [],
        'not_found_in_csv': []
    }
    
    # Gestisco il caso speciale di Ospedale San Giovanni di Dio (più strutture con lo stesso nome)
    if 'Ospedale San Giovanni di Dio' in facility_names:
        facility_names.remove('Ospedale San Giovanni di Dio')
        
        # Ottengo tutte le strutture con questo nome
        with app.app_context():
            with Session(db.engine) as session:
                facilities = session.query(MedicalFacility).filter(
                    MedicalFacility.name == 'Ospedale San Giovanni di Dio'
                ).all()
                
                for facility in facilities:
                    facility_names.append((facility.name, facility.city))
    
    # Verifico ogni struttura
    for facility_item in facility_names:
        if isinstance(facility_item, tuple):
            facility_name, city = facility_item
        else:
            facility_name, city = facility_item, None
        
        # Ottengo i rating dal database
        db_result = get_db_ratings(facility_name, city)
        if db_result is None:
            results['not_found_in_db'].append(facility_name)
            continue
            
        facility_id, db_ratings = db_result
        
        # Ottengo i rating dal CSV
        csv_ratings = get_csv_ratings(csv_file, facility_name, city)
        if csv_ratings is None:
            results['not_found_in_csv'].append(facility_name)
            continue
        
        # Confronto i rating
        discrepancies = compare_ratings(facility_name, db_ratings, csv_ratings)
        
        # Aggiorno i risultati
        results['total_checked'] += 1
        
        if discrepancies:
            results['with_discrepancies'] += 1
            facility_display = facility_name
            if city:
                facility_display = f"{facility_name} ({city})"
                
            for discrepancy in discrepancies:
                discrepancy['facility'] = facility_display
                results['discrepancies'].append(discrepancy)
    
    return results

def print_results(results):
    """
    Stampa i risultati della verifica
    
    Args:
        results: Risultati della verifica
    """
    print("\n============================================")
    print("         RISULTATI DELLA VERIFICA          ")
    print("============================================")
    
    print(f"\nStrutture verificate: {results['total_checked']}")
    print(f"Strutture con discrepanze: {results['with_discrepancies']}")
    
    if results['not_found_in_db']:
        print("\nStrutture non trovate nel database:")
        for facility in results['not_found_in_db']:
            print(f"- {facility}")
    
    if results['not_found_in_csv']:
        print("\nStrutture non trovate nel CSV:")
        for facility in results['not_found_in_csv']:
            print(f"- {facility}")
    
    if results['discrepancies']:
        print("\nDiscrepanze trovate:")
        
        # Raggruppo le discrepanze per struttura
        discrepancies_by_facility = {}
        for discrepancy in results['discrepancies']:
            facility = discrepancy['facility']
            if facility not in discrepancies_by_facility:
                discrepancies_by_facility[facility] = []
            discrepancies_by_facility[facility].append(discrepancy)
        
        # Stampo le discrepanze per struttura
        for facility, discrepancies in discrepancies_by_facility.items():
            print(f"\n--- {facility} ---")
            for discrepancy in discrepancies:
                specialty = discrepancy['specialty']
                db_value = discrepancy['db_value']
                csv_value = discrepancy['csv_value']
                note = discrepancy['note']
                print(f"  {specialty}: DB={db_value}, CSV={csv_value} ({note})")
    else:
        print("\nNessuna discrepanza trovata! Tutte le strutture verificate hanno i valori corretti.")
    
    print("\n============================================")

if __name__ == "__main__":
    csv_file = "./attached_assets/medical_facilities_full_ratings.csv"
    
    print(f"Verifica delle specifiche strutture dal file {csv_file}...")
    
    # Eseguo la verifica
    results = check_specific_facilities(csv_file, FACILITIES_TO_CHECK)
    
    # Stampo i risultati
    print_results(results)