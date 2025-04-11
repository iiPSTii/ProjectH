"""
Script per verificare che tutte le strutture nel database abbiano i valori corretti dal CSV.

Questo script verifica ogni struttura nel database confrontandola con i dati nel CSV
e stampa un rapporto dettagliato delle discrepanze trovate.
"""

import csv
import logging
import sys
import time
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

# Mappatura delle strutture con nomi diversi tra CSV e DB
FACILITY_MAPPINGS = {
    'IDI - Istituto Dermopatico dell\'Immacolata': 'IDI',
    'Ospedale San Paolo - Savona': 'Ospedale San Paolo',
    'Ospedale San Paolo - Milano': 'Ospedale San Paolo',
    'Ospedale San Martino - Belluno': 'Ospedale San Martino',
    'AORN Dei Colli - Monaldi': 'AORN Dei Colli',
    'AORN Dei Colli - Cotugno': 'AORN Dei Colli',
    'Ospedale Morgagni - Pierantoni': 'Ospedale Morgagni',
    'Ospedale di Gubbio - Gualdo Tadino': 'Ospedale di Gubbio'
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
            city = row.get('City', '')
            key = f"{facility_name}|{city}"
            
            # Converto i dati delle specialità in float, ignorando valori non validi
            for specialty in CSV_SPECIALTIES:
                if specialty in row and row[specialty].strip():
                    try:
                        row[specialty] = float(row[specialty])
                    except ValueError:
                        logger.warning(f"Valore non valido per {facility_name}, {specialty}: {row[specialty]}")
                        row[specialty] = None
            
            facilities_data[key] = row
    
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

def find_csv_facility_name(db_facility_name, db_city, csv_data):
    """
    Trova il nome della struttura nel CSV che corrisponde al nome nel database
    
    Args:
        db_facility_name: Nome della struttura nel database
        db_city: Città della struttura nel database
        csv_data: Dati CSV
        
    Returns:
        str: Chiave della struttura nel CSV, o None se non trovata
    """
    # Caso 1: Corrispondenza esatta con nome e città
    key = f"{db_facility_name}|{db_city}"
    if key in csv_data:
        return key
    
    # Caso 2: Corrispondenza usando solo il nome (ignora la città)
    for csv_key in csv_data.keys():
        csv_name, csv_city = csv_key.split('|')
        if csv_name == db_facility_name:
            return csv_key
    
    # Caso 3: Mappatura specifica per strutture con nomi diversi
    if db_facility_name in FACILITY_MAPPINGS:
        mapped_name = FACILITY_MAPPINGS[db_facility_name]
        for csv_key in csv_data.keys():
            csv_name, csv_city = csv_key.split('|')
            if csv_name == mapped_name:
                return csv_key
    
    # Caso 4: Corrispondenza parziale
    for csv_key in csv_data.keys():
        csv_name, csv_city = csv_key.split('|')
        # Se il nome nel DB è contenuto nel nome CSV o viceversa
        if db_facility_name in csv_name or csv_name in db_facility_name:
            return csv_key
    
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
    csv_key = find_csv_facility_name(facility.name, facility.city, csv_data)
    if not csv_key:
        csv_key = f"{facility.name}|{facility.city}"
        stats['discrepancies'].append({
            'facility': facility.name,
            'city': facility.city,
            'error': 'Struttura non trovata nel CSV'
        })
        return stats
    
    # Ottengo le specialità dal database
    db_specialties = get_facility_specialties(facility.id)
    
    # Ottengo i dati dal CSV
    csv_facility_data = csv_data[csv_key]
    csv_name, csv_city = csv_key.split('|')
    
    # Per ogni specialità nel CSV
    for csv_column, db_name in SPECIALTY_MAPPING.items():
        stats['total'] += 1
        
        # Ottengo il valore dal CSV
        csv_value = csv_facility_data.get(csv_column)
        
        # Ottengo il valore dal database
        db_value = db_specialties.get(db_name)
        
        # Se entrambi i valori sono None o vuoti, li considero uguali
        if (csv_value is None or csv_value == '') and (db_value is None or db_value == ''):
            stats['match'] += 1
            continue
        
        # Se il valore nel CSV è None o vuoto ma nel DB esiste
        if (csv_value is None or csv_value == '') and db_value is not None and db_value != '':
            stats['missing_csv'] += 1
            stats['discrepancies'].append({
                'facility': facility.name,
                'city': facility.city,
                'specialty': db_name,
                'csv_value': None,
                'db_value': db_value
            })
            continue
        
        # Se il valore nel DB è None o vuoto ma nel CSV esiste
        if (db_value is None or db_value == '') and csv_value is not None and csv_value != '':
            stats['missing_db'] += 1
            stats['discrepancies'].append({
                'facility': facility.name,
                'city': facility.city,
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
                'city': facility.city,
                'specialty': db_name,
                'csv_value': csv_value,
                'db_value': db_value
            })
        else:
            stats['match'] += 1
    
    return stats

def verify_all_facilities(csv_file, batch_size=50, limit=None):
    """
    Verifica le valutazioni per tutte le strutture con supporto per batch
    
    Args:
        csv_file: Il percorso del file CSV
        batch_size: Dimensione del batch
        limit: Limite opzionale sul numero di strutture da verificare
        
    Returns:
        dict: Statistiche complessive
    """
    # Carico i dati dal CSV
    csv_data = load_csv_data(csv_file)
    
    # Ottengo tutte le strutture dal database
    facilities = get_all_facilities()
    total_facilities = len(facilities)
    logger.info(f"Trovate {total_facilities} strutture nel database")
    
    # Limito il numero di strutture se richiesto
    if limit and limit < total_facilities:
        facilities = facilities[:limit]
        logger.info(f"Limitata la verifica a {limit} strutture")
    
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
        'discrepancies_by_facility': defaultdict(list),
        'facilities_not_in_csv': []
    }
    
    # Verifico le strutture in batch
    for i in range(0, len(facilities), batch_size):
        batch = facilities[i:i+batch_size]
        logger.info(f"Verifico batch {i//batch_size + 1}/{(len(facilities) + batch_size - 1)//batch_size}")
        
        for facility in batch:
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
            for discrepancy in stats['discrepancies']:
                if 'error' in discrepancy:
                    total_stats['facilities_not_in_csv'].append(discrepancy['facility'])
                else:
                    total_stats['facilities_with_discrepancies'] += 1
                    key = f"{discrepancy['facility']} ({discrepancy['city']})"
                    total_stats['discrepancies_by_facility'][key].append(discrepancy)
        
        # Aggiorno e stampo il progresso
        print(f"Strutture verificate: {total_stats['verified_facilities']}/{total_stats['total_facilities']} "
              f"({total_stats['verified_facilities']/total_stats['total_facilities']*100:.1f}%)")
    
    return total_stats

def generate_report(stats, output_file=None):
    """
    Genera un rapporto dettagliato delle discrepanze
    
    Args:
        stats: Statistiche della verifica
        output_file: File di output opzionale
    """
    report = []
    
    # Titolo
    report.append("=============================================")
    report.append("       RAPPORTO DI VERIFICA STRUTTURE       ")
    report.append("=============================================\n")
    
    # Statistiche generali
    report.append(f"Strutture verificate: {stats['verified_facilities']} / {stats['total_facilities']}")
    
    if stats['total_ratings'] > 0:
        match_percent = stats['matching_ratings'] / stats['total_ratings'] * 100
        mismatch_percent = stats['mismatched_ratings'] / stats['total_ratings'] * 100
        missing_db_percent = stats['missing_in_db'] / stats['total_ratings'] * 100
        missing_csv_percent = stats['missing_in_csv'] / stats['total_ratings'] * 100
    else:
        match_percent = mismatch_percent = missing_db_percent = missing_csv_percent = 0
    
    report.append(f"Valutazioni totali verificate: {stats['total_ratings']}")
    report.append(f"Valutazioni corrette: {stats['matching_ratings']} ({match_percent:.2f}%)")
    report.append(f"Valutazioni non corrispondenti: {stats['mismatched_ratings']} ({mismatch_percent:.2f}%)")
    report.append(f"Valutazioni mancanti nel database: {stats['missing_in_db']} ({missing_db_percent:.2f}%)")
    report.append(f"Valutazioni mancanti nel CSV: {stats['missing_in_csv']} ({missing_csv_percent:.2f}%)")
    report.append(f"Strutture con discrepanze: {len(stats['discrepancies_by_facility'])}")
    report.append(f"Strutture non trovate nel CSV: {len(stats['facilities_not_in_csv'])}\n")
    
    # Dettagli delle discrepanze per struttura
    if stats['discrepancies_by_facility']:
        report.append("=============================================")
        report.append("         DETTAGLIO DELLE DISCREPANZE        ")
        report.append("=============================================\n")
        
        for facility_name, discrepancies in stats['discrepancies_by_facility'].items():
            report.append(f"--- {facility_name} ---")
            
            for discrepancy in discrepancies:
                specialty = discrepancy.get('specialty', 'N/A')
                csv_value = discrepancy.get('csv_value', 'N/A')
                db_value = discrepancy.get('db_value', 'N/A')
                
                report.append(f"  {specialty}: CSV={csv_value}, DB={db_value}")
            
            report.append("")
    
    # Strutture non trovate nel CSV
    if stats['facilities_not_in_csv']:
        report.append("=============================================")
        report.append("      STRUTTURE NON TROVATE NEL CSV         ")
        report.append("=============================================\n")
        
        for facility_name in stats['facilities_not_in_csv']:
            report.append(f"- {facility_name}")
        
        report.append("")
    
    # Conclusione
    report.append("=============================================")
    if stats['mismatched_ratings'] > 0 or stats['missing_in_db'] > 0 or stats['missing_in_csv'] > 0:
        report.append("CONCLUSIONE: Sono state trovate discrepanze.")
        report.append("Per correggere tutte le discrepanze, eseguire:")
        report.append("python fix_all_discrepancies.py")
    else:
        report.append("CONCLUSIONE: Tutte le valutazioni sono corrette!")
    report.append("=============================================")
    
    # Stampo il rapporto
    full_report = "\n".join(report)
    print(full_report)
    
    # Salvo il rapporto su file se richiesto
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(full_report)
        print(f"\nRapporto salvato in {output_file}")
    
    return full_report

if __name__ == "__main__":
    csv_file = "./attached_assets/medical_facilities_full_ratings.csv"
    output_file = "verification_report.txt"
    
    print(f"Verifica delle valutazioni per tutte le strutture dal file {csv_file}...")
    start_time = time.time()
    
    # Eseguo la verifica
    stats = verify_all_facilities(csv_file)
    
    # Genero e stampo il rapporto
    generate_report(stats, output_file)
    
    # Stampo il tempo di esecuzione
    execution_time = time.time() - start_time
    print(f"\nTempo di esecuzione: {execution_time:.2f} secondi")