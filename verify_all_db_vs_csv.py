#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script completo e dettagliato per verificare TUTTE le strutture nel database

Questo script confronta ogni struttura nel database con i dati nel CSV originale,
procedendo con batch molto piccoli per evitare timeout. Genera un report dettagliato
delle discrepanze trovate e suggerisce come correggerle.
"""

import csv
import logging
import sys
import argparse
import time
import os
from datetime import datetime

import sqlalchemy
from sqlalchemy.orm import Session

sys.path.append(".")
from app import app, db
from models import MedicalFacility, Specialty, FacilitySpecialty, DatabaseStatus

# Configurazione del logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("verify_all.log"),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

# Mappatura delle colonne CSV ai nomi delle specialità nel database
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
    Carica tutti i dati dal CSV in un dizionario per facilità di accesso
    
    Args:
        csv_file: Percorso del file CSV
        
    Returns:
        dict: Dizionario con chiave (nome, città) -> dati della struttura
    """
    logger.info(f"Caricamento dati dal file CSV {csv_file}")
    csv_data = {}
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                facility_name = row['Name of the facility']
                city = row['City'] if 'City' in row else None
                
                # Creo una chiave composta da nome e città per gestire strutture con lo stesso nome
                key = (facility_name, city)
                
                ratings = {}
                for csv_name, db_name in SPECIALTY_MAPPING.items():
                    if csv_name in row and row[csv_name].strip():
                        try:
                            ratings[db_name] = float(row[csv_name])
                        except ValueError:
                            logger.warning(f"Valore non valido per {facility_name}, {csv_name}: {row[csv_name]}")
                
                csv_data[key] = ratings
        
        logger.info(f"Caricati dati per {len(csv_data)} strutture dal CSV")
        return csv_data
    
    except Exception as e:
        logger.error(f"Errore durante il caricamento del CSV: {str(e)}")
        return {}

def get_all_facilities(session, offset=0, batch_size=10):
    """
    Ottiene un batch di strutture dal database
    
    Args:
        session: Sessione database
        offset: Offset per la paginazione
        batch_size: Dimensione del batch
        
    Returns:
        list: Lista di oggetti MedicalFacility
    """
    facilities = session.query(MedicalFacility).order_by(
        MedicalFacility.id
    ).offset(offset).limit(batch_size).all()
    
    return facilities

def get_facility_specialties(session, facility_id):
    """
    Ottiene tutte le specialità e i relativi rating per una struttura
    
    Args:
        session: Sessione database
        facility_id: ID della struttura
        
    Returns:
        dict: Dizionario con nome specialità -> rating
    """
    sql_query = """
    SELECT s.name, fs.quality_rating
    FROM facility_specialty fs
    JOIN specialties s ON fs.specialty_id = s.id
    WHERE fs.facility_id = :facility_id
    """
    result = session.execute(sqlalchemy.text(sql_query), {"facility_id": facility_id})
    
    specialties = {}
    for row in result:
        specialty_name, rating = row
        if rating is not None:  # Includo solo rating non NULL
            specialties[specialty_name] = float(rating)
    
    return specialties

def find_csv_facility(db_facility_name, db_city, csv_data):
    """
    Trova i dati nel CSV corrispondenti a una struttura nel database
    
    Args:
        db_facility_name: Nome della struttura nel database
        db_city: Città della struttura nel database
        csv_data: Dati CSV
        
    Returns:
        tuple: Chiave della struttura nel CSV e dati corrispondenti, o (None, None) se non trovata
    """
    # Prima provo la corrispondenza esatta
    key = (db_facility_name, db_city)
    if key in csv_data:
        return key, csv_data[key]
    
    # Gestione dei casi speciali
    if db_facility_name == 'Ospedale di Gubbio - Gualdo Tadino':
        key = ('Ospedale di Gubbio', 'Gualdo Tadino - Gubbio')
        if key in csv_data:
            return key, csv_data[key]
    
    if db_facility_name == 'IDI - Istituto Dermopatico dell\'Immacolata':
        key = ('IDI', 'Istituto Dermopatico dell\'Immacolata - Roma')
        if key in csv_data:
            return key, csv_data[key]
        key = ('IDI', None)
        if key in csv_data:
            return key, csv_data[key]
    
    # Se non trovo corrispondenze, provo a cercare solo per nome
    for (name, city), data in csv_data.items():
        if name == db_facility_name:
            return (name, city), data
    
    # Nessuna corrispondenza trovata
    return None, None

def compare_specialty_ratings(db_ratings, csv_ratings):
    """
    Confronta i rating delle specialità tra database e CSV
    
    Args:
        db_ratings: Rating dal database
        csv_ratings: Rating dal CSV
        
    Returns:
        dict: Dizionario con statistiche e discrepanze
    """
    stats = {
        'total_specialties': 0,
        'matching': 0,
        'different': 0,
        'missing_in_db': 0,
        'missing_in_csv': 0,
        'discrepancies': []
    }
    
    # Controllo tutte le specialità nel CSV
    for specialty, csv_rating in csv_ratings.items():
        stats['total_specialties'] += 1
        
        if specialty in db_ratings:
            db_rating = db_ratings[specialty]
            # Uso una piccola tolleranza per gli errori di arrotondamento
            if abs(db_rating - csv_rating) <= 0.01:
                stats['matching'] += 1
            else:
                stats['different'] += 1
                stats['discrepancies'].append({
                    'specialty': specialty,
                    'db_rating': db_rating,
                    'csv_rating': csv_rating,
                    'note': 'Valori diversi'
                })
        else:
            stats['missing_in_db'] += 1
            stats['discrepancies'].append({
                'specialty': specialty,
                'db_rating': None,
                'csv_rating': csv_rating,
                'note': 'Mancante nel database'
            })
    
    # Controllo le specialità nel database che potrebbero non essere nel CSV
    for specialty, db_rating in db_ratings.items():
        if specialty not in csv_ratings and specialty in SPECIALTY_MAPPING.values():
            stats['total_specialties'] += 1
            stats['missing_in_csv'] += 1
            stats['discrepancies'].append({
                'specialty': specialty,
                'db_rating': db_rating,
                'csv_rating': None,
                'note': 'Mancante nel CSV'
            })
    
    return stats

def verify_facilities_batch(csv_data, session, offset, batch_size):
    """
    Verifica un batch di strutture
    
    Args:
        csv_data: Dati dal CSV
        session: Sessione database
        offset: Offset per la paginazione
        batch_size: Dimensione del batch
        
    Returns:
        dict: Statistiche sulla verifica
    """
    facilities = get_all_facilities(session, offset, batch_size)
    
    batch_stats = {
        'facilities_checked': 0,
        'facilities_with_discrepancies': 0,
        'total_specialties': 0,
        'matching_specialties': 0,
        'different_specialties': 0,
        'missing_in_db': 0,
        'missing_in_csv': 0,
        'facility_discrepancies': [],
        'not_found_in_csv': []
    }
    
    for facility in facilities:
        batch_stats['facilities_checked'] += 1
        
        # Ottengo le specialità dal database
        db_ratings = get_facility_specialties(session, facility.id)
        
        # Cerco la struttura nel CSV
        csv_key, csv_ratings = find_csv_facility(facility.name, facility.city, csv_data)
        
        if csv_ratings:
            # Confronto i rating
            comparison = compare_specialty_ratings(db_ratings, csv_ratings)
            
            # Aggiorno le statistiche totali
            batch_stats['total_specialties'] += comparison['total_specialties']
            batch_stats['matching_specialties'] += comparison['matching']
            batch_stats['different_specialties'] += comparison['different']
            batch_stats['missing_in_db'] += comparison['missing_in_db']
            batch_stats['missing_in_csv'] += comparison['missing_in_csv']
            
            # Se ci sono discrepanze, aggiungo questa struttura alla lista
            if comparison['discrepancies']:
                batch_stats['facilities_with_discrepancies'] += 1
                
                facility_info = {
                    'id': facility.id,
                    'name': facility.name,
                    'city': facility.city,
                    'csv_key': csv_key,
                    'discrepancies': comparison['discrepancies']
                }
                
                batch_stats['facility_discrepancies'].append(facility_info)
        else:
            batch_stats['not_found_in_csv'].append({
                'id': facility.id,
                'name': facility.name,
                'city': facility.city
            })
    
    return batch_stats

def verify_all_facilities(csv_file, batch_size=10, output_file=None):
    """
    Verifica tutte le strutture, procedendo a batch
    
    Args:
        csv_file: Percorso del file CSV
        batch_size: Dimensione del batch
        output_file: File di output per il report (opzionale)
        
    Returns:
        dict: Statistiche complessive
    """
    logger.info(f"Inizio verifica di tutte le strutture con batch di {batch_size}")
    
    # Carico i dati dal CSV
    csv_data = load_csv_data(csv_file)
    if not csv_data:
        logger.error("Impossibile caricare i dati dal CSV")
        return None
    
    # Statistiche totali
    stats = {
        'start_time': datetime.now(),
        'total_facilities_checked': 0,
        'total_facilities_with_discrepancies': 0,
        'total_specialties_checked': 0,
        'total_matching_specialties': 0,
        'total_different_specialties': 0,
        'total_missing_in_db': 0,
        'total_missing_in_csv': 0,
        'all_facility_discrepancies': [],
        'not_found_in_csv': []
    }
    
    offset = 0
    more_facilities = True
    
    with app.app_context():
        with Session(db.engine) as session:
            # Ottengo il numero totale di strutture
            total_facilities = session.query(sqlalchemy.func.count(MedicalFacility.id)).scalar()
            logger.info(f"Trovate {total_facilities} strutture nel database")
            
            # Processo le strutture a batch
            while more_facilities:
                logger.info(f"Verifica batch {offset//batch_size + 1}, offset {offset}")
                
                batch_stats = verify_facilities_batch(csv_data, session, offset, batch_size)
                
                # Aggiorno le statistiche totali
                stats['total_facilities_checked'] += batch_stats['facilities_checked']
                stats['total_facilities_with_discrepancies'] += batch_stats['facilities_with_discrepancies']
                stats['total_specialties_checked'] += batch_stats['total_specialties']
                stats['total_matching_specialties'] += batch_stats['matching_specialties']
                stats['total_different_specialties'] += batch_stats['different_specialties']
                stats['total_missing_in_db'] += batch_stats['missing_in_db']
                stats['total_missing_in_csv'] += batch_stats['missing_in_csv']
                
                # Aggiungo le discrepanze di questo batch alla lista totale
                stats['all_facility_discrepancies'].extend(batch_stats['facility_discrepancies'])
                stats['not_found_in_csv'].extend(batch_stats['not_found_in_csv'])
                
                # Controllo se ci sono altre strutture da processare
                if batch_stats['facilities_checked'] < batch_size:
                    more_facilities = False
                
                # Aggiorno l'offset
                offset += batch_size
                
                # Stampo un aggiornamento
                progress = min(100, (offset / total_facilities) * 100)
                logger.info(f"Progresso: {progress:.2f}% - Controllate {stats['total_facilities_checked']} strutture")
                
                # Piccola pausa per evitare di sovraccaricare il sistema
                time.sleep(0.1)
    
    # Aggiungo il tempo totale di esecuzione
    stats['end_time'] = datetime.now()
    stats['execution_time'] = stats['end_time'] - stats['start_time']
    
    # Genero il report
    if output_file:
        generate_report(stats, output_file)
    
    return stats

def generate_report(stats, output_file):
    """
    Genera un report dettagliato delle discrepanze
    
    Args:
        stats: Statistiche della verifica
        output_file: File di output
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("============================================\n")
            f.write("       REPORT VERIFICA DATABASE VS CSV      \n")
            f.write("============================================\n\n")
            
            f.write(f"Data verifica: {stats['start_time']}\n")
            f.write(f"Tempo di esecuzione: {stats['execution_time']}\n\n")
            
            f.write("STATISTICHE GENERALI\n")
            f.write("--------------------\n")
            f.write(f"Strutture verificate: {stats['total_facilities_checked']}\n")
            f.write(f"Strutture con discrepanze: {stats['total_facilities_with_discrepancies']} ({(stats['total_facilities_with_discrepancies']/stats['total_facilities_checked']*100):.2f}%)\n")
            f.write(f"Specialità verificate: {stats['total_specialties_checked']}\n")
            f.write(f"Specialità corrispondenti: {stats['total_matching_specialties']} ({(stats['total_matching_specialties']/stats['total_specialties_checked']*100):.2f}%)\n")
            f.write(f"Specialità con valori diversi: {stats['total_different_specialties']} ({(stats['total_different_specialties']/stats['total_specialties_checked']*100):.2f}%)\n")
            f.write(f"Specialità mancanti nel database: {stats['total_missing_in_db']} ({(stats['total_missing_in_db']/stats['total_specialties_checked']*100):.2f}%)\n")
            f.write(f"Specialità mancanti nel CSV: {stats['total_missing_in_csv']} ({(stats['total_missing_in_csv']/stats['total_specialties_checked']*100):.2f}%)\n\n")
            
            # Aggiungo dettagli sulle strutture non trovate nel CSV
            if stats['not_found_in_csv']:
                f.write("STRUTTURE NON TROVATE NEL CSV\n")
                f.write("------------------------------\n")
                for facility in stats['not_found_in_csv']:
                    f.write(f"ID: {facility['id']}, Nome: {facility['name']}, Città: {facility['city']}\n")
                f.write("\n")
            
            # Aggiungo dettagli sulle discrepanze
            if stats['all_facility_discrepancies']:
                f.write("DETTAGLIO DELLE DISCREPANZE\n")
                f.write("---------------------------\n")
                
                for facility in stats['all_facility_discrepancies']:
                    f.write(f"\nStruttura: {facility['name']}\n")
                    f.write(f"Città: {facility['city']}\n")
                    f.write(f"ID: {facility['id']}\n")
                    f.write(f"Chiave CSV: {facility['csv_key']}\n")
                    f.write("Discrepanze:\n")
                    
                    for disc in facility['discrepancies']:
                        f.write(f"  - {disc['specialty']}: DB={disc['db_rating']}, CSV={disc['csv_rating']} ({disc['note']})\n")
                    
                    f.write("---------------------------\n")
            
            f.write("\n============================================\n")
            
        logger.info(f"Report salvato in {output_file}")
    
    except Exception as e:
        logger.error(f"Errore durante la generazione del report: {str(e)}")

def print_stats(stats):
    """
    Stampa un riassunto delle statistiche
    
    Args:
        stats: Statistiche della verifica
    """
    print("\n============================================")
    print("       RIASSUNTO VERIFICA DATABASE VS CSV      ")
    print("============================================\n")
    
    print(f"Strutture verificate: {stats['total_facilities_checked']}")
    print(f"Strutture con discrepanze: {stats['total_facilities_with_discrepancies']} ({(stats['total_facilities_with_discrepancies']/stats['total_facilities_checked']*100):.2f}%)")
    print(f"Specialità verificate: {stats['total_specialties_checked']}")
    print(f"Specialità corrispondenti: {stats['total_matching_specialties']} ({(stats['total_matching_specialties']/stats['total_specialties_checked']*100):.2f}%)")
    print(f"Specialità con valori diversi: {stats['total_different_specialties']} ({(stats['total_different_specialties']/stats['total_specialties_checked']*100):.2f}%)")
    print(f"Specialità mancanti nel database: {stats['total_missing_in_db']} ({(stats['total_missing_in_db']/stats['total_specialties_checked']*100):.2f}%)")
    print(f"Specialità mancanti nel CSV: {stats['total_missing_in_csv']} ({(stats['total_missing_in_csv']/stats['total_specialties_checked']*100):.2f}%)")
    
    print(f"\nTempo di esecuzione: {stats['execution_time']}")
    
    # Mostro un riassunto delle discrepanze
    if stats['all_facility_discrepancies']:
        print("\nStrutture con discrepanze:")
        
        for i, facility in enumerate(stats['all_facility_discrepancies'], 1):
            print(f"{i}. {facility['name']} ({facility['city']}): {len(facility['discrepancies'])} discrepanze")
            
            # Limito il numero di strutture da mostrare
            if i >= 5 and len(stats['all_facility_discrepancies']) > 10:
                remaining = len(stats['all_facility_discrepancies']) - i
                print(f"... e altre {remaining} strutture (vedi il report completo)")
                break
    
    print("\n============================================")

def parse_arguments():
    """
    Analizza gli argomenti da linea di comando
    
    Returns:
        argparse.Namespace: Argomenti analizzati
    """
    parser = argparse.ArgumentParser(description='Verifica tutte le strutture nel database')
    parser.add_argument('--csv', dest='csv_file', default='./attached_assets/medical_facilities_full_ratings.csv',
                        help='Percorso del file CSV con i dati delle strutture')
    parser.add_argument('--batch-size', dest='batch_size', type=int, default=5,
                        help='Numero di strutture da verificare per batch')
    parser.add_argument('--output', dest='output_file', default='report_verifica_db.txt',
                        help='File di output per il report dettagliato')
    parser.add_argument('--offset', dest='offset', type=int, default=0,
                        help='Offset iniziale per la paginazione (in numero di batch)')
    parser.add_argument('--max-batches', dest='max_batches', type=int, default=None,
                        help='Numero massimo di batch da processare')
    
    return parser.parse_args()

def verify_all_facilities_with_offset(csv_file, batch_size, output_file, offset=0, max_batches=None):
    """
    Verifica tutte le strutture, procedendo a batch con supporto per offset
    
    Args:
        csv_file: Percorso del file CSV
        batch_size: Dimensione del batch
        output_file: File di output per il report
        offset: Offset iniziale (in numero di batch)
        max_batches: Numero massimo di batch da processare
        
    Returns:
        dict: Statistiche complessive
    """
    logger.info(f"Inizio verifica con offset {offset} batch e max_batches {max_batches}")
    
    # Carico i dati dal CSV
    csv_data = load_csv_data(csv_file)
    if not csv_data:
        logger.error("Impossibile caricare i dati dal CSV")
        return None
    
    # Statistiche totali
    stats = {
        'start_time': datetime.now(),
        'total_facilities_checked': 0,
        'total_facilities_with_discrepancies': 0,
        'total_specialties_checked': 0,
        'total_matching_specialties': 0,
        'total_different_specialties': 0,
        'total_missing_in_db': 0,
        'total_missing_in_csv': 0,
        'all_facility_discrepancies': [],
        'not_found_in_csv': []
    }
    
    # Calcolo l'offset reale in numero di strutture
    real_offset = offset * batch_size
    
    batches_processed = 0
    more_facilities = True
    
    with app.app_context():
        with Session(db.engine) as session:
            # Ottengo il numero totale di strutture
            total_facilities = session.query(sqlalchemy.func.count(MedicalFacility.id)).scalar()
            logger.info(f"Trovate {total_facilities} strutture nel database")
            
            # Processo le strutture a batch
            while more_facilities and (max_batches is None or batches_processed < max_batches):
                logger.info(f"Verifica batch {offset + batches_processed + 1}, offset {real_offset + (batches_processed * batch_size)}")
                
                # Ottengo il batch
                batch_stats = verify_facilities_batch(
                    csv_data, 
                    session, 
                    real_offset + (batches_processed * batch_size), 
                    batch_size
                )
                
                # Aggiorno le statistiche totali
                stats['total_facilities_checked'] += batch_stats['facilities_checked']
                stats['total_facilities_with_discrepancies'] += batch_stats['facilities_with_discrepancies']
                stats['total_specialties_checked'] += batch_stats['total_specialties']
                stats['total_matching_specialties'] += batch_stats['matching_specialties']
                stats['total_different_specialties'] += batch_stats['different_specialties']
                stats['total_missing_in_db'] += batch_stats['missing_in_db']
                stats['total_missing_in_csv'] += batch_stats['missing_in_csv']
                
                # Aggiungo le discrepanze di questo batch alla lista totale
                stats['all_facility_discrepancies'].extend(batch_stats['facility_discrepancies'])
                stats['not_found_in_csv'].extend(batch_stats['not_found_in_csv'])
                
                # Controllo se ci sono altre strutture da processare
                if batch_stats['facilities_checked'] < batch_size:
                    more_facilities = False
                
                # Incremento il contatore dei batch
                batches_processed += 1
                
                # Stampo un aggiornamento
                progress = min(100, ((real_offset + (batches_processed * batch_size)) / total_facilities) * 100)
                logger.info(f"Progresso: {progress:.2f}% - Controllate {stats['total_facilities_checked']} strutture")
                
                # Piccola pausa per evitare di sovraccaricare il sistema
                time.sleep(0.1)
    
    # Aggiungo il tempo totale di esecuzione
    stats['end_time'] = datetime.now()
    stats['execution_time'] = stats['end_time'] - stats['start_time']
    
    # Genero il report
    if output_file:
        generate_report(stats, output_file)
    
    return stats

if __name__ == "__main__":
    # Analizzo gli argomenti
    args = parse_arguments()
    
    # Verifico tutte le strutture
    print(f"Verifica di tutte le strutture dal file {args.csv_file} con batch di {args.batch_size}...")
    print(f"Offset: {args.offset} batch, Max batches: {args.max_batches}")
    
    stats = verify_all_facilities_with_offset(
        args.csv_file, 
        args.batch_size, 
        args.output_file,
        args.offset,
        args.max_batches
    )
    
    if stats:
        # Stampo un riassunto
        print_stats(stats)
    else:
        print("Errore durante la verifica. Controlla il log per maggiori dettagli.")