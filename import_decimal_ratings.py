"""
Script per importare valutazioni decimali per strutture mediche

Questo script importa i punteggi di qualità con supporto per valori decimali da un file CSV.
Caratteristiche:
- Supporto completo per valori decimali (es. 3.7, 4.2)
- Backup automatico del database prima dell'importazione
- Elaborazione a batch per evitare timeout
- Report dettagliato delle modifiche

Uso:
  python import_decimal_ratings.py <file_csv> [--batch-size 20] [--skip-backup]

Formato CSV richiesto:
  Prima colonna: 'Name of the facility' (nome della struttura)
  Colonne successive: nomi delle specialità (es. 'Cardiologia', 'Oncologia', ecc.)
"""

import csv
import sys
import argparse
import logging
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import Session
from app import app, db
from models import MedicalFacility, Specialty, FacilitySpecialty
from backup_database import backup_database

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"decimal_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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

def load_csv_data(csv_file):
    """
    Carica i dati dal CSV in un dizionario per facilità di accesso
    
    Args:
        csv_file: Percorso del file CSV
        
    Returns:
        dict: Dizionario con chiave (nome struttura) -> dati
    """
    facilities_data = {}
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # Verifica le colonne presenti
            columns = reader.fieldnames
            if 'Name of the facility' not in columns:
                logger.error("Errore: Colonna 'Name of the facility' non trovata nel CSV")
                return {}
                
            specialty_columns = [col for col in columns if col in SPECIALTY_MAPPING]
            if not specialty_columns:
                logger.error(f"Errore: Nessuna colonna di specialità trovata nel CSV. Colonne disponibili: {columns}")
                return {}
                
            logger.info(f"Colonne di specialità trovate: {specialty_columns}")
            
            # Carica i dati
            for row in reader:
                facility_name = row['Name of the facility']
                
                # Converto i dati delle specialità in float, gestendo valori non validi
                for specialty in SPECIALTY_MAPPING.keys():
                    if specialty in row and row[specialty].strip():
                        try:
                            # Gestisco sia punti che virgole come separatori decimali
                            value = row[specialty].replace(',', '.')
                            row[specialty] = float(value)
                            
                            # Limito i valori all'intervallo 1-5
                            if row[specialty] > 5.0:
                                logger.warning(f"Valore superiore a 5 per {facility_name}, {specialty}: {row[specialty]}, impostato a 5.0")
                                row[specialty] = 5.0
                            elif row[specialty] < 1.0:
                                logger.warning(f"Valore inferiore a 1 per {facility_name}, {specialty}: {row[specialty]}, impostato a 1.0")
                                row[specialty] = 1.0
                                
                        except ValueError:
                            logger.warning(f"Valore non valido per {facility_name}, {specialty}: {row[specialty]}")
                            row[specialty] = None
                
                facilities_data[facility_name] = row
        
        logger.info(f"Caricate {len(facilities_data)} strutture dal CSV")
        return facilities_data
        
    except Exception as e:
        logger.error(f"Errore durante il caricamento del CSV: {str(e)}")
        return {}

def get_specialty_id_by_name(session, name):
    """
    Ottiene l'ID di una specialità dal nome
    
    Args:
        session: Sessione database
        name: Nome della specialità
        
    Returns:
        int: ID della specialità, o None se non trovata
    """
    # Cerca corrispondenza esatta nel DB
    specialty = session.query(Specialty).filter(
        Specialty.name == SPECIALTY_MAPPING.get(name, name)
    ).first()
    
    # Se non trovata, prova una ricerca fuzzy
    if not specialty and name in SPECIALTY_MAPPING:
        specialty = session.query(Specialty).filter(
            Specialty.name.ilike(f"%{SPECIALTY_MAPPING[name]}%")
        ).first()
    
    return specialty.id if specialty else None

def get_facility_by_name(session, name):
    """
    Cerca una struttura nel database per nome
    
    Args:
        session: Sessione database
        name: Nome della struttura
        
    Returns:
        MedicalFacility: Struttura trovata, o None
    """
    # Prima tenta una ricerca esatta
    facility = session.query(MedicalFacility).filter(
        MedicalFacility.name == name
    ).first()
    
    # Se non trovata, prova una ricerca fuzzy
    if not facility:
        logger.info(f"Struttura non trovata con nome esatto: '{name}', provo ricerca fuzzy")
        facility = session.query(MedicalFacility).filter(
            MedicalFacility.name.ilike(f"%{name}%")
        ).first()
        
        if facility:
            logger.info(f"Struttura trovata con ricerca fuzzy: {facility.name} (ID: {facility.id})")
    
    return facility

def get_all_facilities(session, offset=0, batch_size=20):
    """
    Ottiene un batch di strutture dal database
    
    Args:
        session: Sessione database
        offset: Offset per la paginazione
        batch_size: Dimensione del batch
        
    Returns:
        list: Lista di oggetti MedicalFacility
    """
    return session.query(MedicalFacility).order_by(MedicalFacility.id).offset(offset).limit(batch_size).all()

def get_facility_specialties(session, facility_id):
    """
    Ottiene tutte le specialità e i relativi rating per una struttura
    
    Args:
        session: Sessione database
        facility_id: ID della struttura
        
    Returns:
        dict: Dizionario con nome specialità -> (rating, specialty_id)
    """
    query = """
    SELECT s.name, fs.quality_rating, s.id
    FROM facility_specialty fs
    JOIN specialties s ON fs.specialty_id = s.id
    WHERE fs.facility_id = :facility_id
    """
    result = session.execute(text(query), {"facility_id": facility_id})
    
    specialties = {}
    for row in result:
        specialty_name, rating, specialty_id = row
        specialties[specialty_name] = (rating, specialty_id)
    
    return specialties

def update_facility_rating(session, facility_id, specialty_id, new_rating, old_rating=None):
    """
    Aggiorna o crea un rating per una struttura
    
    Args:
        session: Sessione database
        facility_id: ID della struttura
        specialty_id: ID della specialità
        new_rating: Nuovo valore del rating
        old_rating: Rating precedente (opzionale)
        
    Returns:
        bool: True se l'aggiornamento è avvenuto con successo
    """
    try:
        # Controllo se esiste già
        facility_specialty = session.query(FacilitySpecialty).filter_by(
            facility_id=facility_id,
            specialty_id=specialty_id
        ).first()
        
        if facility_specialty:
            # Aggiorno solo se è cambiato (tolleranza di 0.001 per errori di arrotondamento)
            if old_rating is None or abs(facility_specialty.quality_rating - new_rating) > 0.001:
                old_value = facility_specialty.quality_rating
                facility_specialty.quality_rating = new_rating
                logger.info(f"Aggiornato rating: struttura {facility_id}, specialità {specialty_id}: {old_value} -> {new_rating}")
                return True
            else:
                # Nessuna modifica necessaria
                return False
        else:
            # Creo un nuovo record
            new_relationship = FacilitySpecialty(
                facility_id=facility_id,
                specialty_id=specialty_id,
                quality_rating=new_rating
            )
            session.add(new_relationship)
            logger.info(f"Creato nuovo rating: struttura {facility_id}, specialità {specialty_id}: {new_rating}")
            return True
            
    except Exception as e:
        logger.error(f"Errore durante l'aggiornamento del rating: {str(e)}")
        session.rollback()
        return False

def process_facility_batch(session, facilities, csv_data, stats):
    """
    Elabora un batch di strutture
    
    Args:
        session: Sessione database
        facilities: Lista di strutture da processare
        csv_data: Dati dal CSV
        stats: Dizionario per le statistiche
        
    Returns:
        dict: Statistiche aggiornate
    """
    try:
        for facility in facilities:
            # Cerca la struttura nei dati CSV
            facility_data = csv_data.get(facility.name)
            
            if not facility_data:
                # Se non trovata con il nome esatto, prova a cercare stringhe simili
                matches = [name for name in csv_data.keys() if name.lower() in facility.name.lower() or facility.name.lower() in name.lower()]
                
                if matches:
                    # Usa la prima corrispondenza trovata
                    facility_data = csv_data.get(matches[0])
                    logger.info(f"Struttura '{facility.name}' trovata nel CSV come '{matches[0]}'")
                else:
                    stats['non_trovate'] += 1
                    continue
            
            # Ottieni le specialità attuali della struttura
            db_specialties = get_facility_specialties(session, facility.id)
            
            # Verifica e aggiorna i rating
            for specialty_name, db_value in SPECIALTY_MAPPING.items():
                if specialty_name in facility_data and facility_data[specialty_name] is not None:
                    # Il CSV ha un valore per questa specialità
                    new_rating = facility_data[specialty_name]
                    
                    # Ottieni l'ID della specialità
                    specialty_id = get_specialty_id_by_name(session, specialty_name)
                    if not specialty_id:
                        logger.warning(f"Specialità non trovata: {specialty_name}")
                        stats['specialita_non_trovate'] += 1
                        continue
                    
                    # Verifica se esiste già un rating nel DB
                    current_rating = None
                    for db_specialty_name, (rating, sid) in db_specialties.items():
                        if sid == specialty_id:
                            current_rating = rating
                            break
                    
                    # Aggiorna il rating
                    if update_facility_rating(session, facility.id, specialty_id, new_rating, current_rating):
                        if current_rating is None:
                            stats['aggiunti'] += 1
                        else:
                            stats['aggiornati'] += 1
            
            stats['processate'] += 1
            
        session.commit()
        return stats
    except Exception as e:
        logger.error(f"Errore durante l'elaborazione del batch: {str(e)}")
        session.rollback()
        return stats

def update_database_status(message):
    """
    Aggiorna lo stato del database
    
    Args:
        message: Messaggio di stato
    """
    with app.app_context():
        with Session(db.engine) as session:
            # Controlla se esiste già uno stato
            query = "SELECT value FROM app_status WHERE key = 'db_status'"
            existing = session.execute(text(query)).first()
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            new_status = f"{timestamp}: {message}"
            
            if existing:
                session.execute(
                    text("UPDATE app_status SET value = :value WHERE key = 'db_status'"),
                    {"value": new_status}
                )
            else:
                session.execute(
                    text("INSERT INTO app_status (key, value) VALUES ('db_status', :value)"),
                    {"value": new_status}
                )
            
            session.commit()
            logger.info(f"Stato database aggiornato: {new_status}")

def import_decimal_ratings(csv_file, batch_size=20, skip_backup=False):
    """
    Importa rating decimali da un file CSV
    
    Args:
        csv_file: Percorso del file CSV
        batch_size: Dimensione del batch
        skip_backup: Se True, salta il backup del database
        
    Returns:
        dict: Statistiche sull'importazione
    """
    if not skip_backup:
        logger.info("Creazione backup del database...")
        try:
            backup_database()
            logger.info("Backup completato con successo")
        except Exception as e:
            logger.error(f"Errore durante la creazione del backup: {str(e)}")
            logger.error("Operazione interrotta per sicurezza")
            return None
    
    csv_data = load_csv_data(csv_file)
    if not csv_data:
        logger.error("Nessun dato valido trovato nel CSV")
        return None
    
    stats = {
        'processate': 0,
        'aggiornati': 0,
        'aggiunti': 0,
        'non_trovate': 0,
        'specialita_non_trovate': 0,
        'errori': 0
    }
    
    with app.app_context():
        with Session(db.engine) as session:
            # Conta il totale delle strutture
            total_facilities = session.query(MedicalFacility).count()
            logger.info(f"Totale strutture nel database: {total_facilities}")
            
            offset = 0
            while True:
                logger.info(f"Elaborazione batch: offset={offset}, batch_size={batch_size}")
                facilities = get_all_facilities(session, offset, batch_size)
                
                if not facilities:
                    logger.info("Nessuna altra struttura da elaborare")
                    break
                
                batch_stats = process_facility_batch(session, facilities, csv_data, stats)
                stats.update(batch_stats)
                
                logger.info(f"Stato attuale: processate={stats['processate']}, aggiornati={stats['aggiornati']}, aggiunti={stats['aggiunti']}")
                
                offset += batch_size
                
                # Aggiorna lo stato del database ogni 5 batch
                if offset % (batch_size * 5) == 0:
                    progress = round((offset / total_facilities) * 100) if total_facilities > 0 else 0
                    update_database_status(f"Importazione rating decimali in corso... {progress}% completato")
    
    # Aggiorna lo stato finale del database
    status_msg = (f"Importazione completata. Strutture processate: {stats['processate']}, "
                 f"rating aggiornati: {stats['aggiornati']}, rating aggiunti: {stats['aggiunti']}")
    update_database_status(status_msg)
    
    return stats

def print_stats(stats):
    """
    Stampa un report delle statistiche
    
    Args:
        stats: Dizionario con le statistiche
    """
    if not stats:
        print("\n❌ Importazione fallita.")
        return
        
    print("\n" + "="*50)
    print("📊 REPORT IMPORTAZIONE RATING DECIMALI")
    print("="*50)
    print(f"✅ Strutture processate: {stats['processate']}")
    print(f"🔄 Rating aggiornati: {stats['aggiornati']}")
    print(f"➕ Rating aggiunti: {stats['aggiunti']}")
    print(f"⚠️ Strutture non trovate: {stats['non_trovate']}")
    print(f"⚠️ Specialità non trovate: {stats['specialita_non_trovate']}")
    print(f"❌ Errori: {stats['errori']}")
    print("="*50)
    
    # Calcola percentuale di successo
    total_operations = stats['aggiornati'] + stats['aggiunti'] + stats['non_trovate'] + stats['errori']
    success_rate = (stats['aggiornati'] + stats['aggiunti']) / total_operations * 100 if total_operations > 0 else 0
    print(f"📈 Tasso di successo: {success_rate:.1f}%")
    print("="*50)

def parse_arguments():
    """
    Analizza gli argomenti da linea di comando
    
    Returns:
        dict: Argomenti analizzati
    """
    parser = argparse.ArgumentParser(description='Importa rating decimali per strutture mediche')
    parser.add_argument('csv_file', help='Percorso del file CSV con i rating')
    parser.add_argument('--batch-size', type=int, default=20, help='Numero di strutture da processare per batch')
    parser.add_argument('--skip-backup', action='store_true', help='Salta il backup del database')
    
    return parser.parse_args()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python import_decimal_ratings.py <file_csv> [--batch-size 20] [--skip-backup]")
        sys.exit(1)
    
    args = parse_arguments()
    
    logger.info(f"Avvio importazione da {args.csv_file} (batch_size={args.batch_size})")
    stats = import_decimal_ratings(args.csv_file, args.batch_size, args.skip_backup)
    
    print_stats(stats)