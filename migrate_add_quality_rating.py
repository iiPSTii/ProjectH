"""
Migrazione per aggiungere il campo quality_rating alla tabella facility_specialty

Questo script crea una nuova colonna quality_rating nella tabella esistente
facility_specialty per memorizzare i punteggi di qualità specifici per ogni specialità.
"""

import logging
from sqlalchemy import text
from app import app, db

# Setup logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def migrate_add_quality_rating():
    """
    Aggiunge la colonna quality_rating alla tabella facility_specialty
    se non esiste già.
    """
    with app.app_context():
        try:
            # Verifica se la colonna esiste già
            logger.info("Verifica dell'esistenza della colonna quality_rating...")
            result = db.session.execute(
                text("SELECT column_name FROM information_schema.columns "
                    "WHERE table_name='facility_specialty' AND column_name='quality_rating'")
            ).fetchone()
            
            if result:
                logger.info("La colonna quality_rating esiste già nella tabella facility_specialty.")
                return False
            
            # Aggiungi la colonna
            logger.info("Aggiunta della colonna quality_rating alla tabella facility_specialty...")
            db.session.execute(
                text("ALTER TABLE facility_specialty ADD COLUMN quality_rating FLOAT")
            )
            db.session.commit()
            
            logger.info("Colonna quality_rating aggiunta con successo!")
            return True
            
        except Exception as e:
            logger.error(f"Errore durante la migrazione: {e}")
            db.session.rollback()
            raise

def migrate_add_status_log_table():
    """
    Crea la tabella database_status_log per tener traccia delle modifiche
    al database se non esiste già.
    """
    with app.app_context():
        try:
            # Verifica se la tabella esiste già
            logger.info("Verifica dell'esistenza della tabella database_status_log...")
            result = db.session.execute(
                text("SELECT to_regclass('database_status_log')")
            ).scalar()
            
            if result:
                logger.info("La tabella database_status_log esiste già.")
                return False
            
            # Crea la tabella
            logger.info("Creazione della tabella database_status_log...")
            db.session.execute(text("""
                CREATE TABLE database_status_log (
                    id SERIAL PRIMARY KEY,
                    status VARCHAR(50) NOT NULL,
                    message VARCHAR(500),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            db.session.commit()
            
            logger.info("Tabella database_status_log creata con successo!")
            return True
            
        except Exception as e:
            logger.error(f"Errore durante la creazione della tabella di log: {e}")
            db.session.rollback()
            raise

if __name__ == "__main__":
    logger.info("Inizio della migrazione del database...")
    
    # Aggiungi la colonna quality_rating
    quality_rating_added = migrate_add_quality_rating()
    
    # Crea la tabella di log dello stato
    status_log_added = migrate_add_status_log_table()
    
    if quality_rating_added or status_log_added:
        logger.info("Migrazione completata con successo!")
    else:
        logger.info("Nessuna modifica necessaria, il database è già aggiornato.")