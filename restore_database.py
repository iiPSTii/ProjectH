"""
Script per ripristinare il database da un backup

Questo script ripristina il database da un backup ZIP creato precedentemente,
annullando tutte le modifiche fatte dopo la creazione del backup.

Uso:
  python restore_database.py <file_backup.zip>
"""

import sys
import os
import zipfile
import csv
import logging
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import Session
from app import app, db
from models import Region, Specialty, MedicalFacility, FacilitySpecialty

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"restore_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_backup(backup_file):
    """
    Estrae il contenuto di un backup ZIP in una cartella temporanea
    
    Args:
        backup_file: Percorso del file ZIP di backup
        
    Returns:
        str: Percorso della cartella temporanea con i file estratti, o None in caso di errore
    """
    try:
        # Estrae il nome base del file di backup (senza estensione)
        base_name = os.path.splitext(os.path.basename(backup_file))[0]
        
        # Se la directory esiste già, la usiamo direttamente
        if os.path.exists(base_name) and os.path.isdir(base_name):
            # Verifica se esiste la sottocartella con lo stesso nome (caso speciale)
            nested_dir = os.path.join(base_name, base_name)
            if os.path.exists(nested_dir) and os.path.isdir(nested_dir):
                logger.info(f"Individuata struttura nidificata: {nested_dir}")
                return nested_dir
            logger.info(f"La cartella {base_name} esiste già, uso i file esistenti")
            return base_name
            
        # Altrimenti, estraiamo il contenuto del backup
        with zipfile.ZipFile(backup_file, 'r') as zip_ref:
            zip_ref.extractall(base_name)
        
        logger.info(f"Backup estratto nella cartella {base_name}")
        
        # Verifica se esiste una sottocartella con lo stesso nome (caso speciale)
        nested_dir = os.path.join(base_name, base_name)
        if os.path.exists(nested_dir) and os.path.isdir(nested_dir):
            logger.info(f"Individuata struttura nidificata: {nested_dir}")
            return nested_dir
            
        return base_name
    except Exception as e:
        logger.error(f"Errore durante l'estrazione del backup: {str(e)}")
        return None

def truncate_tables():
    """
    Svuota tutte le tabelle nel database mantenendo la struttura
    
    Returns:
        bool: True se l'operazione è riuscita, False altrimenti
    """
    try:
        with app.app_context():
            with Session(db.engine) as session:
                # Prima elimina i record di FacilitySpecialty (tabella di relazione)
                session.execute(text("DELETE FROM facility_specialty"))
                
                # Poi elimina le strutture mediche
                session.execute(text("DELETE FROM medical_facilities"))
                
                # Infine elimina le specialità e le regioni
                session.execute(text("DELETE FROM specialties"))
                session.execute(text("DELETE FROM regions"))
                
                session.commit()
                
                logger.info("Tabelle svuotate con successo")
                return True
    except Exception as e:
        logger.error(f"Errore durante lo svuotamento delle tabelle: {str(e)}")
        return False

def restore_table(session, csv_file, model_class):
    """
    Ripristina i dati di una tabella da un file CSV
    
    Args:
        session: Sessione del database
        csv_file: Percorso del file CSV con i dati
        model_class: Classe del modello da ripristinare
        
    Returns:
        int: Numero di record ripristinati
    """
    try:
        count = 0
        
        # Ottiene i nomi delle colonne del modello
        model_columns = [column.name for column in model_class.__table__.columns]
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # Crea un nuovo record con solo le colonne presenti nel modello
                record_data = {col: row[col] for col in row if col in model_columns}
                
                # Converti i tipi di dati se necessario
                for col in record_data:
                    if record_data[col] == '':
                        record_data[col] = None
                    elif record_data[col] == 'True':
                        record_data[col] = True
                    elif record_data[col] == 'False':
                        record_data[col] = False
                
                # Crea e aggiungi la nuova istanza
                new_record = model_class(**record_data)
                session.add(new_record)
                count += 1
                
                # Commit ogni 100 record per ridurre l'uso di memoria
                if count % 100 == 0:
                    session.commit()
                    logger.debug(f"Ripristinati {count} record...")
            
            session.commit()
            
        return count
    except Exception as e:
        logger.error(f"Errore durante il ripristino della tabella: {str(e)}")
        session.rollback()
        return 0

def restore_facility_specialty(session, csv_file):
    """
    Ripristina le relazioni tra strutture e specialità
    
    Args:
        session: Sessione del database
        csv_file: Percorso del file CSV con i dati
        
    Returns:
        int: Numero di relazioni ripristinate
    """
    try:
        count = 0
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # Crea un nuovo record
                facility_id = int(row['facility_id']) if row['facility_id'] else None
                specialty_id = int(row['specialty_id']) if row['specialty_id'] else None
                quality_rating = float(row['quality_rating']) if row['quality_rating'] and row['quality_rating'] != 'None' else None
                
                if facility_id is None or specialty_id is None:
                    logger.warning(f"ID mancante: facility_id={facility_id}, specialty_id={specialty_id}")
                    continue
                
                # Crea e aggiungi la nuova istanza
                new_record = FacilitySpecialty(
                    facility_id=facility_id,
                    specialty_id=specialty_id,
                    quality_rating=quality_rating
                )
                session.add(new_record)
                count += 1
                
                # Commit ogni 100 record per ridurre l'uso di memoria
                if count % 100 == 0:
                    session.commit()
                    logger.debug(f"Ripristinate {count} relazioni...")
            
            session.commit()
            
        return count
    except Exception as e:
        logger.error(f"Errore durante il ripristino delle relazioni: {str(e)}")
        session.rollback()
        return 0

def restore_database(backup_file):
    """
    Ripristina l'intero database da un backup
    
    Args:
        backup_file: Percorso del file ZIP di backup
        
    Returns:
        dict: Statistiche sul ripristino
    """
    stats = {
        'regions': 0,
        'specialties': 0,
        'facilities': 0,
        'relations': 0,
        'status': 'failed'
    }
    
    # Estrai il backup
    backup_dir = extract_backup(backup_file)
    if not backup_dir:
        return stats
    
    # Verifica che i file necessari esistano
    required_files = [
        os.path.join(backup_dir, 'regions.csv'),
        os.path.join(backup_dir, 'specialties.csv'),
        os.path.join(backup_dir, 'medical_facilities.csv'),
        os.path.join(backup_dir, 'facility_specialties.csv')
    ]
    
    for file_path in required_files:
        if not os.path.exists(file_path):
            logger.error(f"File richiesto non trovato: {file_path}")
            return stats
    
    # Svuota le tabelle esistenti
    if not truncate_tables():
        return stats
    
    # Ripristina i dati
    with app.app_context():
        with Session(db.engine) as session:
            try:
                # Ripristina le tabelle in ordine di dipendenza
                stats['regions'] = restore_table(
                    session, 
                    os.path.join(backup_dir, 'regions.csv'),
                    Region
                )
                
                stats['specialties'] = restore_table(
                    session, 
                    os.path.join(backup_dir, 'specialties.csv'),
                    Specialty
                )
                
                stats['facilities'] = restore_table(
                    session, 
                    os.path.join(backup_dir, 'medical_facilities.csv'),
                    MedicalFacility
                )
                
                stats['relations'] = restore_facility_specialty(
                    session,
                    os.path.join(backup_dir, 'facility_specialties.csv')
                )
                
                # Aggiorna lo stato del database (se esiste la tabella)
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                status_message = f"{timestamp}: Database ripristinato da backup {os.path.basename(backup_file)}"
                
                try:
                    # Verifica se la tabella esiste
                    check_table = session.execute(text("SELECT to_regclass('app_status')")).scalar()
                    
                    if check_table is not None:
                        # La tabella esiste, aggiorna lo stato
                        session.execute(
                            text("UPDATE app_status SET value = :value WHERE key = 'db_status'"),
                            {"value": status_message}
                        )
                    else:
                        # La tabella non esiste, creala e inserisci lo stato
                        logger.info("Tabella app_status non trovata, la creo")
                        session.execute(text(
                            "CREATE TABLE IF NOT EXISTS app_status (key TEXT PRIMARY KEY, value TEXT)"
                        ))
                        session.execute(
                            text("INSERT INTO app_status (key, value) VALUES ('db_status', :value)"),
                            {"value": status_message}
                        )
                except Exception as e:
                    # Se c'è un errore nell'aggiornamento dello stato, lo loggo ma non fallisco
                    logger.warning(f"Impossibile aggiornare lo stato del database: {str(e)}")
                
                session.commit()
                stats['status'] = 'success'
                logger.info(f"Database ripristinato con successo")
                
                return stats
            except Exception as e:
                logger.error(f"Errore durante il ripristino del database: {str(e)}")
                session.rollback()
                return stats

def print_summary(stats, backup_file):
    """
    Stampa un riepilogo dell'operazione di ripristino
    
    Args:
        stats: Statistiche del ripristino
        backup_file: File di backup utilizzato
    """
    print("\n" + "="*60)
    print(" "*15 + "RIEPILOGO RIPRISTINO DATABASE")
    print("="*60)
    
    if stats['status'] == 'success':
        print(f"✅ Ripristino completato con successo dal backup:")
        print(f"   {os.path.basename(backup_file)}")
        print("\n📊 DATI RIPRISTINATI:")
        print(f"🌍 Regioni: {stats['regions']}")
        print(f"🏥 Specialità: {stats['specialties']}")
        print(f"🏥 Strutture sanitarie: {stats['facilities']}")
        print(f"🔗 Relazioni struttura-specialità: {stats['relations']}")
    else:
        print(f"❌ Ripristino fallito. Controlla i log per maggiori dettagli.")
    
    print("="*60)
    
    if stats['status'] == 'success':
        print("\nℹ️ Il database è stato ripristinato allo stato precedente all'importazione dei dati decimali.")
        print("   Tutti i valori decimali fittizi sono stati rimossi.")
        print("\n📝 NOTA: Il sistema continua a supportare valori decimali, ma ora contiene")
        print("         solo i dati originali con valori interi come richiesto.")
    
    print("="*60)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python restore_database.py <file_backup.zip>")
        print("\nBackup disponibili:")
        for backup in sorted([f for f in os.listdir('.') if f.startswith('database_backup_') and f.endswith('.zip')], reverse=True):
            print(f"  - {backup}")
        sys.exit(1)
    
    backup_file = sys.argv[1]
    if not os.path.exists(backup_file):
        print(f"Errore: Il file {backup_file} non esiste.")
        sys.exit(1)
    
    logger.info(f"Avvio ripristino database da {backup_file}")
    stats = restore_database(backup_file)
    
    print_summary(stats, backup_file)