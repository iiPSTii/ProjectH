#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script per eseguire una verifica completa del database in batch piccoli.

Questo script esegue la verifica di tutte le strutture nel database in batch piccoli,
aggregando i risultati in un unico report finale. È progettato per evitare timeout
e garantire che tutte le strutture vengano verificate.
"""

import os
import sys
import time
import subprocess
import glob
import re
import logging
from datetime import datetime

# Configurazione del logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("run_full_verification.log"),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

# Parametri di configurazione
BATCH_SIZE = 5  # Numero di strutture per batch
MAX_BATCHES_PER_RUN = 10  # Numero massimo di batch per ogni esecuzione
CSV_FILE = "./attached_assets/medical_facilities_full_ratings.csv"
FINAL_REPORT = "final_verification_report.txt"

def run_verification_batch(offset):
    """
    Esegue un batch di verifica
    
    Args:
        offset: Offset di batch da cui iniziare
        
    Returns:
        bool: True se completato con successo, False altrimenti
    """
    # Creo un nome di file output unico con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"report_verify_{timestamp}_offset_{offset}.txt"
    
    # Compongo il comando
    cmd = [
        "python", "verify_all_db_vs_csv.py",
        "--batch-size", str(BATCH_SIZE),
        "--max-batches", str(MAX_BATCHES_PER_RUN),
        "--offset", str(offset),
        "--output", output_file,
        "--csv", CSV_FILE
    ]
    
    logger.info(f"Esecuzione verifica batch: offset={offset}")
    
    # Eseguo il comando
    try:
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        end_time = time.time()
        
        logger.info(f"Batch completato in {end_time - start_time:.2f} secondi")
        
        # Estraggo informazioni utili dall'output
        output = result.stdout
        
        # Cerco informazioni sul numero di strutture verificate
        match = re.search(r"Controllate (\d+) strutture", output)
        if match:
            structures_checked = int(match.group(1))
            logger.info(f"Verificate {structures_checked} strutture in questo batch")
        
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Errore durante l'esecuzione: {e}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Error: {e.stderr}")
        return False

def extract_stats_from_report(report_file):
    """
    Estrae statistiche da un file di report
    
    Args:
        report_file: Percorso del file di report
        
    Returns:
        dict: Statistiche estratte
    """
    stats = {
        'total_facilities_checked': 0,
        'total_facilities_with_discrepancies': 0,
        'total_specialties_checked': 0,
        'total_matching_specialties': 0,
        'total_different_specialties': 0,
        'total_missing_in_db': 0,
        'total_missing_in_csv': 0,
        'facility_discrepancies': []
    }
    
    try:
        with open(report_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # Estraggo le statistiche generali
            match = re.search(r"Strutture verificate: (\d+)", content)
            if match:
                stats['total_facilities_checked'] = int(match.group(1))
            
            match = re.search(r"Strutture con discrepanze: (\d+)", content)
            if match:
                stats['total_facilities_with_discrepancies'] = int(match.group(1))
            
            match = re.search(r"Specialità verificate: (\d+)", content)
            if match:
                stats['total_specialties_checked'] = int(match.group(1))
            
            match = re.search(r"Specialità corrispondenti: (\d+)", content)
            if match:
                stats['total_matching_specialties'] = int(match.group(1))
            
            match = re.search(r"Specialità con valori diversi: (\d+)", content)
            if match:
                stats['total_different_specialties'] = int(match.group(1))
            
            match = re.search(r"Specialità mancanti nel database: (\d+)", content)
            if match:
                stats['total_missing_in_db'] = int(match.group(1))
            
            match = re.search(r"Specialità mancanti nel CSV: (\d+)", content)
            if match:
                stats['total_missing_in_csv'] = int(match.group(1))
            
            # Estraggo i dettagli delle discrepanze
            sections = re.findall(r"Struttura: (.*?)---------------------------", content, re.DOTALL)
            
            for section in sections:
                lines = section.strip().split('\n')
                facility_name = lines[0].strip()
                
                # Salto le righe con informazioni che non mi interessano
                discrepancies = []
                for line in lines:
                    if " DB=" in line and " CSV=" in line:
                        discrepancies.append(line.strip())
                
                if discrepancies:
                    stats['facility_discrepancies'].append({
                        'facility': facility_name,
                        'discrepancies': discrepancies
                    })
        
        return stats
    
    except Exception as e:
        logger.error(f"Errore durante l'estrazione delle statistiche da {report_file}: {str(e)}")
        return stats

def merge_reports():
    """
    Unisce tutti i report in un unico report finale
    
    Returns:
        dict: Statistiche complessive
    """
    # Statistiche totali
    total_stats = {
        'total_facilities_checked': 0,
        'total_facilities_with_discrepancies': 0,
        'total_specialties_checked': 0,
        'total_matching_specialties': 0,
        'total_different_specialties': 0,
        'total_missing_in_db': 0,
        'total_missing_in_csv': 0,
        'all_facility_discrepancies': []
    }
    
    # Trovo tutti i file di report
    report_files = glob.glob("report_verify_*.txt")
    
    logger.info(f"Trovati {len(report_files)} file di report")
    
    for report_file in report_files:
        logger.info(f"Elaborazione report: {report_file}")
        stats = extract_stats_from_report(report_file)
        
        # Aggiorno le statistiche totali
        total_stats['total_facilities_checked'] += stats['total_facilities_checked']
        total_stats['total_facilities_with_discrepancies'] += stats['total_facilities_with_discrepancies']
        total_stats['total_specialties_checked'] += stats['total_specialties_checked']
        total_stats['total_matching_specialties'] += stats['total_matching_specialties']
        total_stats['total_different_specialties'] += stats['total_different_specialties']
        total_stats['total_missing_in_db'] += stats['total_missing_in_db']
        total_stats['total_missing_in_csv'] += stats['total_missing_in_csv']
        
        # Aggiungo le discrepanze di questo report alla lista totale
        total_stats['all_facility_discrepancies'].extend(stats['facility_discrepancies'])
    
    return total_stats

def generate_final_report(stats):
    """
    Genera il report finale con tutte le statistiche
    
    Args:
        stats: Statistiche complessive
    """
    try:
        with open(FINAL_REPORT, 'w', encoding='utf-8') as f:
            f.write("============================================\n")
            f.write("       REPORT FINALE VERIFICA DATABASE      \n")
            f.write("============================================\n\n")
            
            f.write(f"Data: {datetime.now()}\n\n")
            
            f.write("STATISTICHE COMPLESSIVE\n")
            f.write("----------------------\n")
            f.write(f"Strutture verificate: {stats['total_facilities_checked']}\n")
            
            if stats['total_facilities_checked'] > 0:
                percentage = (stats['total_facilities_with_discrepancies'] / stats['total_facilities_checked']) * 100
                f.write(f"Strutture con discrepanze: {stats['total_facilities_with_discrepancies']} ({percentage:.2f}%)\n")
            
            f.write(f"Specialità verificate: {stats['total_specialties_checked']}\n")
            
            if stats['total_specialties_checked'] > 0:
                matching_percentage = (stats['total_matching_specialties'] / stats['total_specialties_checked']) * 100
                different_percentage = (stats['total_different_specialties'] / stats['total_specialties_checked']) * 100
                missing_db_percentage = (stats['total_missing_in_db'] / stats['total_specialties_checked']) * 100
                missing_csv_percentage = (stats['total_missing_in_csv'] / stats['total_specialties_checked']) * 100
                
                f.write(f"Specialità corrispondenti: {stats['total_matching_specialties']} ({matching_percentage:.2f}%)\n")
                f.write(f"Specialità con valori diversi: {stats['total_different_specialties']} ({different_percentage:.2f}%)\n")
                f.write(f"Specialità mancanti nel database: {stats['total_missing_in_db']} ({missing_db_percentage:.2f}%)\n")
                f.write(f"Specialità mancanti nel CSV: {stats['total_missing_in_csv']} ({missing_csv_percentage:.2f}%)\n")
            
            f.write("\n")
            
            # Dettaglio delle discrepanze
            if stats['all_facility_discrepancies']:
                f.write("DETTAGLIO DELLE DISCREPANZE\n")
                f.write("---------------------------\n\n")
                
                for facility in stats['all_facility_discrepancies']:
                    f.write(f"Struttura: {facility['facility']}\n")
                    f.write("Discrepanze:\n")
                    
                    for discrepancy in facility['discrepancies']:
                        f.write(f"  - {discrepancy}\n")
                    
                    f.write("\n")
            else:
                f.write("Nessuna discrepanza trovata.\n")
            
            f.write("\n============================================\n")
        
        logger.info(f"Report finale salvato in {FINAL_REPORT}")
        
    except Exception as e:
        logger.error(f"Errore durante la generazione del report finale: {str(e)}")

def run_full_verification():
    """
    Esegue la verifica completa del database
    """
    print("Avvio verifica completa del database...")
    print(f"Dimensione batch: {BATCH_SIZE}, Max batch per esecuzione: {MAX_BATCHES_PER_RUN}")
    
    offset = 0
    structures_verified = 0
    while True:
        # Eseguo un batch di verifica
        success = run_verification_batch(offset)
        
        if not success:
            logger.error(f"Errore durante la verifica del batch con offset {offset}")
            print(f"Errore durante la verifica. Controllare il log per maggiori dettagli.")
            break
        
        # Incremento l'offset
        offset += MAX_BATCHES_PER_RUN
        
        # Aggiorno le strutture verificate
        structures_verified += BATCH_SIZE * MAX_BATCHES_PER_RUN
        
        print(f"Progresso: verificate circa {structures_verified} strutture")
        
        # Chiedo all'utente se vuole continuare
        while True:
            response = input("Continuare con il prossimo batch? (s/n): ")
            if response.lower() in ["s", "si", "sì", "y", "yes"]:
                break
            elif response.lower() in ["n", "no"]:
                print("Verifica interrotta dall'utente")
                # Genero comunque il report finale
                break
            else:
                print("Risposta non valida. Inserire 's' per continuare o 'n' per terminare.")
        
        if response.lower() in ["n", "no"]:
            break
    
    # Unisco tutti i report
    print("Elaborazione di tutti i report...")
    total_stats = merge_reports()
    
    # Genero il report finale
    generate_final_report(total_stats)
    
    print(f"Verifica completata. Report finale salvato in {FINAL_REPORT}")
    
    return total_stats

if __name__ == "__main__":
    run_full_verification()