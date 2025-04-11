#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script per eseguire una correzione completa del database in batch piccoli.

Questo script esegue la correzione di tutte le strutture nel database in batch piccoli,
aggregando i risultati in un unico report finale. È progettato per evitare timeout
e garantire che tutte le strutture vengano corrette.
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
                        logging.FileHandler("run_full_correction.log"),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

# Parametri di configurazione
BATCH_SIZE = 5  # Numero di strutture per batch
MAX_BATCHES_PER_RUN = 10  # Numero massimo di batch per ogni esecuzione
CSV_FILE = "./attached_assets/medical_facilities_full_ratings.csv"
FINAL_REPORT = "final_correction_report.txt"

def run_correction_batch(offset):
    """
    Esegue un batch di correzione
    
    Args:
        offset: Offset di batch da cui iniziare
        
    Returns:
        bool: True se completato con successo, False altrimenti
    """
    # Creo un nome di file output unico con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"report_correct_{timestamp}_offset_{offset}.txt"
    
    # Compongo il comando
    cmd = [
        "python", "fix_all_db_vs_csv.py",
        "--batch-size", str(BATCH_SIZE),
        "--max-batches", str(MAX_BATCHES_PER_RUN),
        "--offset", str(offset),
        "--output", output_file,
        "--csv", CSV_FILE
    ]
    
    logger.info(f"Esecuzione correzione batch: offset={offset}")
    
    # Eseguo il comando
    try:
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        end_time = time.time()
        
        logger.info(f"Batch completato in {end_time - start_time:.2f} secondi")
        
        # Estraggo informazioni utili dall'output
        output = result.stdout
        
        # Cerco informazioni sul numero di strutture corrette
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
        'total_facilities_updated': 0,
        'total_specialties_checked': 0,
        'total_specialties_updated': 0,
        'total_specialties_added': 0,
        'total_already_correct': 0,
        'facility_updates': []
    }
    
    try:
        with open(report_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # Estraggo le statistiche generali
            match = re.search(r"Strutture verificate: (\d+)", content)
            if match:
                stats['total_facilities_checked'] = int(match.group(1))
            
            match = re.search(r"Strutture aggiornate: (\d+)", content)
            if match:
                stats['total_facilities_updated'] = int(match.group(1))
            
            match = re.search(r"Specialità verificate: (\d+)", content)
            if match:
                stats['total_specialties_checked'] = int(match.group(1))
            
            match = re.search(r"Specialità già corrette: (\d+)", content)
            if match:
                stats['total_already_correct'] = int(match.group(1))
            
            match = re.search(r"Specialità aggiornate: (\d+)", content)
            if match:
                stats['total_specialties_updated'] = int(match.group(1))
            
            match = re.search(r"Specialità aggiunte: (\d+)", content)
            if match:
                stats['total_specialties_added'] = int(match.group(1))
            
            # Estraggo i dettagli degli aggiornamenti
            sections = re.findall(r"Struttura: (.*?)---------------------------", content, re.DOTALL)
            
            for section in sections:
                lines = section.strip().split('\n')
                facility_name = lines[0].strip()
                
                # Salto le righe con informazioni che non mi interessano
                updates = []
                for line in lines:
                    if line.strip().startswith("- "):
                        updates.append(line.strip())
                
                if updates:
                    stats['facility_updates'].append({
                        'facility': facility_name,
                        'updates': updates
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
        'total_facilities_updated': 0,
        'total_specialties_checked': 0,
        'total_specialties_updated': 0,
        'total_specialties_added': 0,
        'total_already_correct': 0,
        'all_facility_updates': []
    }
    
    # Trovo tutti i file di report
    report_files = glob.glob("report_correct_*.txt")
    
    logger.info(f"Trovati {len(report_files)} file di report")
    
    for report_file in report_files:
        logger.info(f"Elaborazione report: {report_file}")
        stats = extract_stats_from_report(report_file)
        
        # Aggiorno le statistiche totali
        total_stats['total_facilities_checked'] += stats['total_facilities_checked']
        total_stats['total_facilities_updated'] += stats['total_facilities_updated']
        total_stats['total_specialties_checked'] += stats['total_specialties_checked']
        total_stats['total_specialties_updated'] += stats['total_specialties_updated']
        total_stats['total_specialties_added'] += stats['total_specialties_added']
        total_stats['total_already_correct'] += stats['total_already_correct']
        
        # Aggiungo gli aggiornamenti di questo report alla lista totale
        total_stats['all_facility_updates'].extend(stats['facility_updates'])
    
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
            f.write("       REPORT FINALE CORREZIONE DATABASE      \n")
            f.write("============================================\n\n")
            
            f.write(f"Data: {datetime.now()}\n\n")
            
            f.write("STATISTICHE COMPLESSIVE\n")
            f.write("----------------------\n")
            f.write(f"Strutture verificate: {stats['total_facilities_checked']}\n")
            
            if stats['total_facilities_checked'] > 0:
                percentage = (stats['total_facilities_updated'] / stats['total_facilities_checked']) * 100
                f.write(f"Strutture aggiornate: {stats['total_facilities_updated']} ({percentage:.2f}%)\n")
            
            f.write(f"Specialità verificate: {stats['total_specialties_checked']}\n")
            
            if stats['total_specialties_checked'] > 0:
                already_correct_percentage = (stats['total_already_correct'] / stats['total_specialties_checked']) * 100
                updated_percentage = (stats['total_specialties_updated'] / stats['total_specialties_checked']) * 100
                added_percentage = (stats['total_specialties_added'] / stats['total_specialties_checked']) * 100
                
                f.write(f"Specialità già corrette: {stats['total_already_correct']} ({already_correct_percentage:.2f}%)\n")
                f.write(f"Specialità aggiornate: {stats['total_specialties_updated']} ({updated_percentage:.2f}%)\n")
                f.write(f"Specialità aggiunte: {stats['total_specialties_added']} ({added_percentage:.2f}%)\n")
            
            f.write("\n")
            
            # Dettaglio degli aggiornamenti
            if stats['all_facility_updates']:
                f.write("DETTAGLIO DEGLI AGGIORNAMENTI\n")
                f.write("----------------------------\n\n")
                
                for facility in stats['all_facility_updates']:
                    f.write(f"Struttura: {facility['facility']}\n")
                    f.write("Aggiornamenti:\n")
                    
                    for update in facility['updates']:
                        f.write(f"{update}\n")
                    
                    f.write("\n")
            else:
                f.write("Nessun aggiornamento effettuato.\n")
            
            f.write("\n============================================\n")
        
        logger.info(f"Report finale salvato in {FINAL_REPORT}")
        
    except Exception as e:
        logger.error(f"Errore durante la generazione del report finale: {str(e)}")

def run_full_correction():
    """
    Esegue la correzione completa del database
    """
    print("Avvio correzione completa del database...")
    print(f"Dimensione batch: {BATCH_SIZE}, Max batch per esecuzione: {MAX_BATCHES_PER_RUN}")
    
    offset = 0
    structures_processed = 0
    while True:
        # Eseguo un batch di correzione
        success = run_correction_batch(offset)
        
        if not success:
            logger.error(f"Errore durante la correzione del batch con offset {offset}")
            print(f"Errore durante la correzione. Controllare il log per maggiori dettagli.")
            break
        
        # Incremento l'offset
        offset += MAX_BATCHES_PER_RUN
        
        # Aggiorno le strutture processate
        structures_processed += BATCH_SIZE * MAX_BATCHES_PER_RUN
        
        print(f"Progresso: processate circa {structures_processed} strutture")
        
        # Chiedo all'utente se vuole continuare
        while True:
            response = input("Continuare con il prossimo batch? (s/n): ")
            if response.lower() in ["s", "si", "sì", "y", "yes"]:
                break
            elif response.lower() in ["n", "no"]:
                print("Correzione interrotta dall'utente")
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
    
    print(f"Correzione completata. Report finale salvato in {FINAL_REPORT}")
    
    return total_stats

if __name__ == "__main__":
    run_full_correction()