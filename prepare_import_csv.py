"""
Script per preparare un file CSV per l'importazione di rating decimali

Questo script converte un file CSV standard con rating interi in un nuovo file CSV
con rating decimali, generando valori casuali ma realistici basati sui rating originali.
È utile per test e dimostrazioni del supporto per rating decimali.

Uso:
  python prepare_import_csv.py <file_csv_input> <file_csv_output>

Formato CSV richiesto:
  Prima colonna: 'Name of the facility' (nome della struttura)
  Colonne successive: nomi delle specialità (es. 'Cardiologia', 'Oncologia', ecc.)
"""

import csv
import sys
import random
import logging
from datetime import datetime

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"prepare_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Specialità supportate (stesse del mappaggio in import_decimal_ratings.py)
SUPPORTED_SPECIALTIES = [
    'Cardiologia',
    'Ortopedia',
    'Oncologia',
    'Neurologia',
    'Urologia',
    'Chirurgia generale',
    'Pediatria',
    'Ginecologia'
]

def add_decimal_precision(original_value):
    """
    Aggiunge precisione decimale a un valore intero
    
    Args:
        original_value: Valore originale come stringa o float
        
    Returns:
        float: Valore con decimali o None se input non valido
    """
    try:
        # Converte stringhe vuote o valori non validi
        if not original_value or original_value == "":
            return None
            
        # Converte il valore originale in float
        base_value = float(str(original_value).replace(',', '.'))
        
        # Arrotonda a un intero
        integer_value = int(base_value)
        
        # Genera un nuovo valore decimale basato sull'intero
        # Mantiene il numero intero ma aggiunge precisione decimale realistica
        if integer_value < 1:
            # Valori inferiori a 1 vengono portati a 1.0-1.4
            decimal_value = round(1.0 + random.random() * 0.4, 1)
        elif integer_value >= 5:
            # Valori 5 o più vengono mantenuti a 5.0
            decimal_value = 5.0
        else:
            # Aggiunge una precisione decimale casuale (+0.0 a +0.9)
            random_decimal = round(random.random() * 0.9, 1)
            decimal_value = round(integer_value + random_decimal, 1)
            
        return decimal_value
    except (ValueError, TypeError) as e:
        logger.warning(f"Errore nella conversione del valore '{original_value}': {str(e)}")
        return None

def convert_csv_to_decimal(input_file, output_file):
    """
    Converte un file CSV con rating interi in un file con rating decimali
    
    Args:
        input_file: Percorso del file CSV di input
        output_file: Percorso del file CSV di output
    
    Returns:
        bool: True se la conversione è avvenuta con successo
    """
    try:
        all_rows = []
        specialty_columns = []
        
        # Legge il file di input
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # Identifica le colonne di specialità
            headers = reader.fieldnames
            if 'Name of the facility' not in headers:
                logger.error("Colonna 'Name of the facility' non trovata nel CSV di input")
                return False
                
            specialty_columns = [col for col in headers if col in SUPPORTED_SPECIALTIES]
            if not specialty_columns:
                logger.error(f"Nessuna colonna di specialità supportata trovata nel CSV. Colonne: {headers}")
                return False
                
            logger.info(f"Specialità trovate: {specialty_columns}")
            
            # Processa ogni riga
            for row in reader:
                # Copia la riga
                new_row = row.copy()
                
                # Modifica i valori delle specialità con valori decimali
                for specialty in specialty_columns:
                    if specialty in row:
                        new_row[specialty] = add_decimal_precision(row[specialty])
                
                all_rows.append(new_row)
                
        if not all_rows:
            logger.error("Nessuna riga trovata nel CSV di input")
            return False
            
        # Scrive il file di output
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(all_rows)
            
        logger.info(f"Conversione completata. Scritte {len(all_rows)} righe nel file {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"Errore durante la conversione del CSV: {str(e)}")
        return False

def print_summary(success, input_file, output_file, start_time):
    """
    Stampa un riepilogo dell'operazione
    
    Args:
        success: Esito dell'operazione
        input_file: File di input
        output_file: File di output
        start_time: Orario di inizio
    """
    duration = datetime.now() - start_time
    
    print("\n" + "="*50)
    print("📊 RIEPILOGO CONVERSIONE CSV")
    print("="*50)
    
    if success:
        print(f"✅ Conversione completata con successo!")
        print(f"📁 File di input: {input_file}")
        print(f"📁 File di output: {output_file}")
        print(f"⏱️ Durata: {duration.total_seconds():.2f} secondi")
    else:
        print(f"❌ Conversione fallita. Controllare i log per maggiori dettagli.")
    
    print("="*50)
    
    if success:
        print("\nPuoi ora importare i rating decimali con il comando:")
        print(f"python import_decimal_ratings.py {output_file}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python prepare_import_csv.py <file_csv_input> <file_csv_output>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    start_time = datetime.now()
    logger.info(f"Avvio conversione da {input_file} a {output_file}")
    
    success = convert_csv_to_decimal(input_file, output_file)
    
    print_summary(success, input_file, output_file, start_time)