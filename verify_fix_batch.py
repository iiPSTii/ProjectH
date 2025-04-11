#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script per verificare e correggere il database in batch piccoli

Questo script esegue piccoli batch di verifica e correzione,
evitando timeout e garantendo che tutti i dati vengano controllati.
"""

import os
import sys
import time
import argparse
import logging
from datetime import datetime

# Configurazione del logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("verify_fix_batch.log"),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

def run_batch(start_offset, batch_size, max_batches, mode="verify"):
    """
    Esegue un batch di verifica o correzione
    
    Args:
        start_offset: Offset iniziale (in numero di strutture)
        batch_size: Dimensione del batch
        max_batches: Numero massimo di batch da eseguire
        mode: "verify" o "fix"
    
    Returns:
        int: Nuovo offset per il prossimo batch
    """
    # Converto l'offset in batch
    batch_offset = start_offset // batch_size
    
    if mode == "verify":
        script = "verify_all_db_vs_csv.py"
    else:
        script = "fix_all_db_vs_csv.py"
    
    # Compongo il comando
    cmd = f"python {script} --batch-size {batch_size} --max-batches {max_batches}"
    
    # Se abbiamo un offset, lo aggiungiamo al comando (da implementare nei rispettivi script)
    if batch_offset > 0:
        cmd += f" --offset {batch_offset}"
    
    # Imposto un nome di file output unico con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"report_{mode}_{timestamp}_offset_{batch_offset}.txt"
    cmd += f" --output {output_file}"
    
    logger.info(f"Esecuzione comando: {cmd}")
    
    # Eseguo il comando
    start_time = time.time()
    exit_code = os.system(cmd)
    end_time = time.time()
    
    # Controllo il risultato
    if exit_code == 0:
        logger.info(f"Comando completato con successo in {end_time - start_time:.2f} secondi")
    else:
        logger.error(f"Comando fallito con codice di uscita {exit_code}")
        # In caso di errore, non avanziamo
        return start_offset
    
    # Calcolo il nuovo offset
    new_offset = start_offset + (batch_size * max_batches)
    
    return new_offset

def main():
    """Funzione principale"""
    parser = argparse.ArgumentParser(description='Verifica e corregge il database in batch piccoli')
    parser.add_argument('--batch-size', type=int, default=2,
                        help='Dimensione del batch (strutture per batch)')
    parser.add_argument('--max-batches', type=int, default=10,
                        help='Numero massimo di batch per esecuzione')
    parser.add_argument('--start-offset', type=int, default=0,
                        help='Offset iniziale (in numero di strutture)')
    parser.add_argument('--mode', choices=['verify', 'fix'], default='verify',
                        help='Modalità: "verify" solo verifica, "fix" applica correzioni')
    parser.add_argument('--max-structures', type=int, default=None,
                        help='Numero massimo di strutture da processare')
    
    args = parser.parse_args()
    
    logger.info(f"Avvio in modalità {args.mode} con batch_size={args.batch_size}, max_batches={args.max_batches}, start_offset={args.start_offset}")
    
    offset = args.start_offset
    structures_processed = 0
    
    while True:
        # Se abbiamo raggiunto il limite massimo di strutture, terminiamo
        if args.max_structures is not None and structures_processed >= args.max_structures:
            logger.info(f"Raggiunto il limite massimo di strutture ({args.max_structures}). Terminazione.")
            break
        
        # Eseguo il batch
        new_offset = run_batch(offset, args.batch_size, args.max_batches, args.mode)
        
        # Calcolo quante strutture abbiamo processato
        batch_structures = new_offset - offset
        structures_processed += batch_structures
        
        # Se non abbiamo avanzato, probabilmente c'è stato un errore o abbiamo finito
        if new_offset == offset:
            logger.warning("Nessun avanzamento nell'offset. Terminazione.")
            break
        
        # Aggiorno l'offset per il prossimo batch
        offset = new_offset
        
        logger.info(f"Processate {structures_processed} strutture. Nuovo offset: {offset}")
        
        # Piccola pausa tra i batch
        logger.info("Pausa di 2 secondi prima del prossimo batch...")
        time.sleep(2)

if __name__ == "__main__":
    main()