"""
Script per l'importazione di ratings da CSV in batch più piccoli

Questo script importa i punteggi di specialità da un file CSV,
ma elabora solo un numero limitato di strutture alla volta per evitare timeout.
"""

import sys
import os
import csv
import logging
from app import app
from import_new_format_ratings import import_ratings_new_format

# Setup logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_file_in_batches(csv_file, start_row=0, num_rows=25):
    """
    Processa un numero limitato di righe dal file CSV.
    
    Args:
        csv_file: Il file CSV originale
        start_row: Riga iniziale da cui partire (0-based)
        num_rows: Numero di righe da processare
        
    Returns:
        tuple: (righe_totali, righe_processate)
    """
    # Leggi tutte le righe dal file originale
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # Salva l'header
        all_rows = list(reader)
    
    total_rows = len(all_rows)
    
    # Determina l'intervallo di righe da processare
    end_row = min(start_row + num_rows, total_rows)
    rows_to_process = all_rows[start_row:end_row]
    
    if not rows_to_process:
        return total_rows, 0
    
    # Crea un file CSV temporaneo con solo l'header e le righe selezionate
    temp_file = f"temp_batch_{start_row}_{end_row}.csv"
    with open(temp_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows_to_process)
    
    try:
        # Usa la funzione esistente per importare i dati dal file temporaneo
        with app.app_context():
            import_ratings_new_format(temp_file)
        
        return total_rows, len(rows_to_process)
    finally:
        # Rimuovi il file temporaneo
        if os.path.exists(temp_file):
            os.remove(temp_file)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python import_in_batches.py path/to/ratings.csv [start_row] [num_rows]")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    
    # Parametri opzionali
    start_row = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    num_rows = int(sys.argv[3]) if len(sys.argv) > 3 else 25
    
    print(f"Importazione batch da {csv_file}, righe {start_row}-{start_row+num_rows}...")
    
    total_rows, processed_rows = process_file_in_batches(csv_file, start_row, num_rows)
    
    print(f"\nImportazione batch completata:")
    print(f"  Righe totali nel file: {total_rows}")
    print(f"  Righe processate: {processed_rows}")
    
    if start_row + processed_rows < total_rows:
        next_start = start_row + processed_rows
        print(f"\nPer continuare con il batch successivo, esegui:")
        print(f"  python import_in_batches.py {csv_file} {next_start} {num_rows}")
    else:
        print("\nImportazione completata! Tutte le righe sono state processate.")