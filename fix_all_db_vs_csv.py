#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script per correggere tutte le discrepanze tra database e CSV

Questo script applica le correzioni per tutte le discrepanze trovate tra
il database e il file CSV originale. Procede con batch molto piccoli per evitare timeout
e crea un backup del database prima di apportare modifiche.
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
import backup_database  # Importo lo script per il backup del database

# Configurazione del logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("fix_all.log"),
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
        dict: Dizionario con nome specialità -> rating e ID specialty
    """
    sql_query = """
    SELECT s.id, s.name, fs.quality_rating, fs.id as fs_id
    FROM facility_specialty fs
    JOIN specialties s ON fs.specialty_id = s.id
    WHERE fs.facility_id = :facility_id
    """
    result = session.execute(sqlalchemy.text(sql_query), {"facility_id": facility_id})
    
    specialties = {}
    for row in result:
        specialty_id, specialty_name, rating, fs_id = row
        specialties[specialty_name] = {
            'specialty_id': specialty_id,
            'rating': float(rating) if rating is not None else None,
            'fs_id': fs_id
        }
    
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

def get_or_create_specialty(session, specialty_name):
    """
    Ottiene o crea una specialità
    
    Args:
        session: Sessione database
        specialty_name: Nome della specialità
        
    Returns:
        Specialty: Oggetto specialità
    """
    specialty = session.query(Specialty).filter(
        Specialty.name == specialty_name
    ).first()
    
    if not specialty:
        specialty = Specialty(name=specialty_name)
        session.add(specialty)
        session.flush()  # Per ottenere l'ID
        
    return specialty

def fix_facility_ratings(session, facility, db_specialties, csv_ratings):
    """
    Corregge i rating di una struttura in base ai dati CSV
    
    Args:
        session: Sessione database
        facility: Oggetto MedicalFacility
        db_specialties: Specialità nel database per questa struttura
        csv_ratings: Rating dal CSV
        
    Returns:
        dict: Statistiche sulle correzioni
    """
    stats = {
        'specialty_checked': 0,
        'updated': 0,
        'added': 0,
        'already_correct': 0,
        'updates': []
    }
    
    # Verifico e aggiorno i rating esistenti
    for specialty_name, csv_rating in csv_ratings.items():
        stats['specialty_checked'] += 1
        
        if specialty_name in db_specialties:
            # La specialità esiste già, verifico se il rating è corretto
            db_info = db_specialties[specialty_name]
            db_rating = db_info['rating']
            
            # Se il rating è diverso, lo aggiorno
            if abs(db_rating - csv_rating) > 0.01:
                # Ottengo il record facility_specialty
                fs = session.get(FacilitySpecialty, db_info['fs_id'])
                if fs:
                    old_rating = fs.quality_rating
                    fs.quality_rating = csv_rating
                    stats['updated'] += 1
                    stats['updates'].append(f"{specialty_name}: {old_rating} -> {csv_rating}")
            else:
                stats['already_correct'] += 1
        else:
            # La specialità non esiste per questa struttura, la aggiungo
            specialty = get_or_create_specialty(session, specialty_name)
            
            fs = FacilitySpecialty(
                facility_id=facility.id,
                specialty_id=specialty.id,
                quality_rating=csv_rating
            )
            session.add(fs)
            stats['added'] += 1
            stats['updates'].append(f"{specialty_name}: Aggiunto con rating {csv_rating}")
    
    return stats

def fix_facilities_batch(csv_data, session, offset, batch_size):
    """
    Corregge un batch di strutture
    
    Args:
        csv_data: Dati dal CSV
        session: Sessione database
        offset: Offset per la paginazione
        batch_size: Dimensione del batch
        
    Returns:
        dict: Statistiche sulle correzioni
    """
    facilities = get_all_facilities(session, offset, batch_size)
    
    batch_stats = {
        'facilities_checked': 0,
        'facilities_updated': 0,
        'total_specialties_checked': 0,
        'total_specialties_updated': 0,
        'total_specialties_added': 0,
        'total_already_correct': 0,
        'facility_updates': [],
        'not_found_in_csv': []
    }
    
    for facility in facilities:
        batch_stats['facilities_checked'] += 1
        facility_updated = False
        
        # Ottengo le specialità dal database
        db_specialties = get_facility_specialties(session, facility.id)
        
        # Cerco la struttura nel CSV
        csv_key, csv_ratings = find_csv_facility(facility.name, facility.city, csv_data)
        
        if csv_ratings:
            # Correggo i rating
            fix_stats = fix_facility_ratings(session, facility, db_specialties, csv_ratings)
            
            # Aggiorno le statistiche totali
            batch_stats['total_specialties_checked'] += fix_stats['specialty_checked']
            batch_stats['total_specialties_updated'] += fix_stats['updated']
            batch_stats['total_specialties_added'] += fix_stats['added']
            batch_stats['total_already_correct'] += fix_stats['already_correct']
            
            # Se ho fatto aggiornamenti, aggiungo questa struttura alla lista
            if fix_stats['updated'] > 0 or fix_stats['added'] > 0:
                facility_updated = True
                batch_stats['facilities_updated'] += 1
                
                facility_info = {
                    'id': facility.id,
                    'name': facility.name,
                    'city': facility.city,
                    'csv_key': csv_key,
                    'updates': fix_stats['updates']
                }
                
                batch_stats['facility_updates'].append(facility_info)
        else:
            batch_stats['not_found_in_csv'].append({
                'id': facility.id,
                'name': facility.name,
                'city': facility.city
            })
    
    # Commit solo alla fine del batch
    session.commit()
    
    return batch_stats

def update_database_status(session, message):
    """
    Aggiorna lo stato del database
    
    Args:
        session: Sessione database
        message: Messaggio di stato
    """
    status = session.query(DatabaseStatus).first()
    if status:
        status.status = "updated"
        status.last_updated = datetime.now()
        status.message = message
        session.commit()
        logger.info(f"Stato database aggiornato: {message}")
    else:
        logger.warning("Stato database non trovato")

def fix_all_facilities(csv_file, batch_size=5, max_batches=None):
    """
    Corregge tutte le strutture, procedendo a batch
    
    Args:
        csv_file: Percorso del file CSV
        batch_size: Dimensione del batch
        max_batches: Numero massimo di batch da processare (opzionale)
        
    Returns:
        dict: Statistiche complessive
    """
    logger.info(f"Inizio correzione di tutte le strutture con batch di {batch_size}")
    
    # Prima eseguo un backup del database
    logger.info("Esecuzione backup del database...")
    backup_database.backup_database()
    
    # Carico i dati dal CSV
    csv_data = load_csv_data(csv_file)
    if not csv_data:
        logger.error("Impossibile caricare i dati dal CSV")
        return None
    
    # Statistiche totali
    stats = {
        'start_time': datetime.now(),
        'total_facilities_checked': 0,
        'total_facilities_updated': 0,
        'total_specialties_checked': 0,
        'total_specialties_updated': 0,
        'total_specialties_added': 0,
        'total_already_correct': 0,
        'all_facility_updates': [],
        'not_found_in_csv': []
    }
    
    offset = 0
    more_facilities = True
    batches_processed = 0
    
    with app.app_context():
        with Session(db.engine) as session:
            # Ottengo il numero totale di strutture
            total_facilities = session.query(sqlalchemy.func.count(MedicalFacility.id)).scalar()
            logger.info(f"Trovate {total_facilities} strutture nel database")
            
            # Processo le strutture a batch
            while more_facilities and (max_batches is None or batches_processed < max_batches):
                logger.info(f"Correzione batch {batches_processed + 1}, offset {offset}")
                
                batch_stats = fix_facilities_batch(csv_data, session, offset, batch_size)
                
                # Aggiorno le statistiche totali
                stats['total_facilities_checked'] += batch_stats['facilities_checked']
                stats['total_facilities_updated'] += batch_stats['facilities_updated']
                stats['total_specialties_checked'] += batch_stats['total_specialties_checked']
                stats['total_specialties_updated'] += batch_stats['total_specialties_updated']
                stats['total_specialties_added'] += batch_stats['total_specialties_added']
                stats['total_already_correct'] += batch_stats['total_already_correct']
                
                # Aggiungo gli aggiornamenti di questo batch alla lista totale
                stats['all_facility_updates'].extend(batch_stats['facility_updates'])
                stats['not_found_in_csv'].extend(batch_stats['not_found_in_csv'])
                
                # Controllo se ci sono altre strutture da processare
                if batch_stats['facilities_checked'] < batch_size:
                    more_facilities = False
                
                # Aggiorno l'offset
                offset += batch_size
                batches_processed += 1
                
                # Stampo un aggiornamento
                progress = min(100, (offset / total_facilities) * 100)
                logger.info(f"Progresso: {progress:.2f}% - Controllate {stats['total_facilities_checked']} strutture")
                
                # Piccola pausa per evitare di sovraccaricare il sistema
                time.sleep(0.1)
            
            # Aggiorno lo stato del database
            update_msg = f"Aggiornate {stats['total_facilities_updated']} strutture, {stats['total_specialties_updated']} specialità aggiornate, {stats['total_specialties_added']} specialità aggiunte"
            update_database_status(session, update_msg)
    
    # Aggiungo il tempo totale di esecuzione
    stats['end_time'] = datetime.now()
    stats['execution_time'] = stats['end_time'] - stats['start_time']
    
    return stats

def generate_report(stats, output_file):
    """
    Genera un report dettagliato delle correzioni
    
    Args:
        stats: Statistiche delle correzioni
        output_file: File di output
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("============================================\n")
            f.write("       REPORT CORREZIONI DATABASE VS CSV      \n")
            f.write("============================================\n\n")
            
            f.write(f"Data correzione: {stats['start_time']}\n")
            f.write(f"Tempo di esecuzione: {stats['execution_time']}\n\n")
            
            f.write("STATISTICHE GENERALI\n")
            f.write("--------------------\n")
            f.write(f"Strutture verificate: {stats['total_facilities_checked']}\n")
            f.write(f"Strutture aggiornate: {stats['total_facilities_updated']} ({(stats['total_facilities_updated']/stats['total_facilities_checked']*100):.2f}%)\n")
            f.write(f"Specialità verificate: {stats['total_specialties_checked']}\n")
            f.write(f"Specialità già corrette: {stats['total_already_correct']} ({(stats['total_already_correct']/stats['total_specialties_checked']*100):.2f}%)\n")
            f.write(f"Specialità aggiornate: {stats['total_specialties_updated']} ({(stats['total_specialties_updated']/stats['total_specialties_checked']*100):.2f}%)\n")
            f.write(f"Specialità aggiunte: {stats['total_specialties_added']} ({(stats['total_specialties_added']/stats['total_specialties_checked']*100):.2f}%)\n\n")
            
            # Aggiungo dettagli sulle strutture non trovate nel CSV
            if stats['not_found_in_csv']:
                f.write("STRUTTURE NON TROVATE NEL CSV\n")
                f.write("------------------------------\n")
                for facility in stats['not_found_in_csv']:
                    f.write(f"ID: {facility['id']}, Nome: {facility['name']}, Città: {facility['city']}\n")
                f.write("\n")
            
            # Aggiungo dettagli sugli aggiornamenti
            if stats['all_facility_updates']:
                f.write("DETTAGLIO DEGLI AGGIORNAMENTI\n")
                f.write("-----------------------------\n")
                
                for facility in stats['all_facility_updates']:
                    f.write(f"\nStruttura: {facility['name']}\n")
                    f.write(f"Città: {facility['city']}\n")
                    f.write(f"ID: {facility['id']}\n")
                    f.write(f"Chiave CSV: {facility['csv_key']}\n")
                    f.write("Aggiornamenti:\n")
                    
                    for update in facility['updates']:
                        f.write(f"  - {update}\n")
                    
                    f.write("---------------------------\n")
            
            f.write("\n============================================\n")
            
        logger.info(f"Report salvato in {output_file}")
    
    except Exception as e:
        logger.error(f"Errore durante la generazione del report: {str(e)}")

def print_stats(stats):
    """
    Stampa un riassunto delle statistiche
    
    Args:
        stats: Statistiche delle correzioni
    """
    print("\n============================================")
    print("       RIASSUNTO CORREZIONI DATABASE VS CSV      ")
    print("============================================\n")
    
    print(f"Strutture verificate: {stats['total_facilities_checked']}")
    print(f"Strutture aggiornate: {stats['total_facilities_updated']} ({(stats['total_facilities_updated']/stats['total_facilities_checked']*100):.2f}%)")
    print(f"Specialità verificate: {stats['total_specialties_checked']}")
    print(f"Specialità già corrette: {stats['total_already_correct']} ({(stats['total_already_correct']/stats['total_specialties_checked']*100):.2f}%)")
    print(f"Specialità aggiornate: {stats['total_specialties_updated']} ({(stats['total_specialties_updated']/stats['total_specialties_checked']*100):.2f}%)")
    print(f"Specialità aggiunte: {stats['total_specialties_added']} ({(stats['total_specialties_added']/stats['total_specialties_checked']*100):.2f}%)")
    
    print(f"\nTempo di esecuzione: {stats['execution_time']}")
    
    # Mostro un riassunto degli aggiornamenti
    if stats['all_facility_updates']:
        print("\nStrutture aggiornate:")
        
        for i, facility in enumerate(stats['all_facility_updates'], 1):
            print(f"{i}. {facility['name']} ({facility['city']}): {len(facility['updates'])} aggiornamenti")
            
            # Limito il numero di strutture da mostrare
            if i >= 5 and len(stats['all_facility_updates']) > 10:
                remaining = len(stats['all_facility_updates']) - i
                print(f"... e altre {remaining} strutture (vedi il report completo)")
                break
    
    print("\n============================================")

def parse_arguments():
    """
    Analizza gli argomenti da linea di comando
    
    Returns:
        argparse.Namespace: Argomenti analizzati
    """
    parser = argparse.ArgumentParser(description='Corregge tutte le strutture nel database')
    parser.add_argument('--csv', dest='csv_file', default='./attached_assets/medical_facilities_full_ratings.csv',
                        help='Percorso del file CSV con i dati delle strutture')
    parser.add_argument('--batch-size', dest='batch_size', type=int, default=3,
                        help='Numero di strutture da correggere per batch')
    parser.add_argument('--max-batches', dest='max_batches', type=int, default=None,
                        help='Numero massimo di batch da processare (opzionale)')
    parser.add_argument('--offset', dest='offset', type=int, default=0,
                        help='Offset iniziale per la paginazione (in numero di batch)')
    parser.add_argument('--output', dest='output_file', default='report_correzioni_db.txt',
                        help='File di output per il report dettagliato')
    
    return parser.parse_args()

def fix_all_facilities_with_offset(csv_file, batch_size, output_file, offset=0, max_batches=None):
    """
    Corregge tutte le strutture, procedendo a batch con supporto per offset
    
    Args:
        csv_file: Percorso del file CSV
        batch_size: Dimensione del batch
        output_file: File di output per il report
        offset: Offset iniziale (in numero di batch)
        max_batches: Numero massimo di batch da processare
        
    Returns:
        dict: Statistiche complessive
    """
    logger.info(f"Inizio correzione con offset {offset} batch e max_batches {max_batches}")
    
    # Prima eseguo un backup del database
    logger.info("Esecuzione backup del database...")
    backup_database.backup_database()
    
    # Carico i dati dal CSV
    csv_data = load_csv_data(csv_file)
    if not csv_data:
        logger.error("Impossibile caricare i dati dal CSV")
        return None
    
    # Statistiche totali
    stats = {
        'start_time': datetime.now(),
        'total_facilities_checked': 0,
        'total_facilities_updated': 0,
        'total_specialties_checked': 0,
        'total_specialties_updated': 0,
        'total_specialties_added': 0,
        'total_already_correct': 0,
        'all_facility_updates': [],
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
                logger.info(f"Correzione batch {offset + batches_processed + 1}, offset {real_offset + (batches_processed * batch_size)}")
                
                # Ottengo e correggo il batch
                batch_stats = fix_facilities_batch(
                    csv_data, 
                    session, 
                    real_offset + (batches_processed * batch_size), 
                    batch_size
                )
                
                # Aggiorno le statistiche totali
                stats['total_facilities_checked'] += batch_stats['facilities_checked']
                stats['total_facilities_updated'] += batch_stats['facilities_updated']
                stats['total_specialties_checked'] += batch_stats['total_specialties_checked']
                stats['total_specialties_updated'] += batch_stats['total_specialties_updated']
                stats['total_specialties_added'] += batch_stats['total_specialties_added']
                stats['total_already_correct'] += batch_stats['total_already_correct']
                
                # Aggiungo gli aggiornamenti di questo batch alla lista totale
                stats['all_facility_updates'].extend(batch_stats['facility_updates'])
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
            
            # Aggiorno lo stato del database
            update_msg = f"Aggiornate {stats['total_facilities_updated']} strutture, {stats['total_specialties_updated']} specialità aggiornate, {stats['total_specialties_added']} specialità aggiunte"
            update_database_status(session, update_msg)
    
    # Aggiungo il tempo totale di esecuzione
    stats['end_time'] = datetime.now()
    stats['execution_time'] = stats['end_time'] - stats['start_time']
    
    return stats

if __name__ == "__main__":
    # Analizzo gli argomenti
    args = parse_arguments()
    
    print(f"Correzione di tutte le strutture dal file {args.csv_file} con batch di {args.batch_size}...")
    print(f"Offset: {args.offset} batch, Max batches: {args.max_batches}")
    
    stats = fix_all_facilities_with_offset(
        args.csv_file, 
        args.batch_size, 
        args.output_file,
        args.offset,
        args.max_batches
    )
    
    if stats:
        # Genero il report
        generate_report(stats, args.output_file)
        
        # Stampo un riassunto
        print_stats(stats)
    else:
        print("Errore durante la correzione. Controlla il log per maggiori dettagli.")