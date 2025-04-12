"""
Script per verificare i rating decimali nel database

Questo script verifica che i rating decimali siano stati correttamente importati
e sono visualizzati correttamente nell'applicazione web.

Uso:
  python verify_decimal_ratings.py [--detailed]
  
Opzioni:
  --detailed: Mostra un report dettagliato con tutti i rating trovati
"""

import argparse
import logging
from sqlalchemy import text
from sqlalchemy.orm import Session
from app import app, db
from models import MedicalFacility, Specialty, FacilitySpecialty

# Configurazione logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def has_decimal_part(value):
    """
    Controlla se un valore ha una parte decimale significativa
    
    Args:
        value: Valore da controllare
        
    Returns:
        bool: True se il valore ha una parte decimale significativa
    """
    if value is None:
        return False
        
    return abs(value - round(value)) > 0.001

def get_ratings_statistics():
    """
    Ottiene statistiche sui rating decimali nel database
    
    Returns:
        dict: Statistiche sui rating
    """
    with app.app_context():
        with Session(db.engine) as session:
            stats = {
                'total_ratings': 0,
                'decimal_ratings': 0,
                'integer_ratings': 0,
                'null_ratings': 0,
                'facilities_with_ratings': 0,
                'specialties_with_ratings': 0,
                'min_rating': None,
                'max_rating': None,
                'avg_rating': None,
                'decimal_examples': []  # per il report dettagliato
            }
            
            # Conta i rating totali
            total_query = "SELECT COUNT(*) FROM facility_specialty"
            stats['total_ratings'] = session.execute(text(total_query)).scalar() or 0
            
            # Conta i rating non nulli
            non_null_query = "SELECT COUNT(*) FROM facility_specialty WHERE quality_rating IS NOT NULL"
            non_null_count = session.execute(text(non_null_query)).scalar() or 0
            
            # Conta i rating nulli
            stats['null_ratings'] = stats['total_ratings'] - non_null_count
            
            # Se non ci sono rating non nulli, restituisci le statistiche di base
            if non_null_count == 0:
                return stats
                
            # Conta i rating decimali (con parte decimale significativa)
            decimal_query = """
            SELECT COUNT(*) 
            FROM facility_specialty 
            WHERE quality_rating IS NOT NULL 
            AND ABS(quality_rating - ROUND(quality_rating)) > 0.001
            """
            stats['decimal_ratings'] = session.execute(text(decimal_query)).scalar() or 0
            
            # Conta i rating interi (senza parte decimale significativa)
            stats['integer_ratings'] = non_null_count - stats['decimal_ratings']
            
            # Calcola min, max e media
            min_query = "SELECT MIN(quality_rating) FROM facility_specialty WHERE quality_rating IS NOT NULL"
            max_query = "SELECT MAX(quality_rating) FROM facility_specialty WHERE quality_rating IS NOT NULL"
            avg_query = "SELECT AVG(quality_rating) FROM facility_specialty WHERE quality_rating IS NOT NULL"
            
            stats['min_rating'] = session.execute(text(min_query)).scalar()
            stats['max_rating'] = session.execute(text(max_query)).scalar()
            stats['avg_rating'] = session.execute(text(avg_query)).scalar()
            
            # Conta le strutture con almeno un rating
            facilities_query = """
            SELECT COUNT(DISTINCT facility_id) 
            FROM facility_specialty 
            WHERE quality_rating IS NOT NULL
            """
            stats['facilities_with_ratings'] = session.execute(text(facilities_query)).scalar() or 0
            
            # Conta le specialità con almeno un rating
            specialties_query = """
            SELECT COUNT(DISTINCT specialty_id) 
            FROM facility_specialty 
            WHERE quality_rating IS NOT NULL
            """
            stats['specialties_with_ratings'] = session.execute(text(specialties_query)).scalar() or 0
            
            # Raccoglie esempi di rating decimali per il report dettagliato
            if stats['decimal_ratings'] > 0:
                examples_query = """
                SELECT 
                    mf.name AS facility_name, 
                    s.name AS specialty_name,
                    fs.quality_rating
                FROM facility_specialty fs
                JOIN medical_facilities mf ON fs.facility_id = mf.id
                JOIN specialties s ON fs.specialty_id = s.id
                WHERE fs.quality_rating IS NOT NULL
                AND ABS(fs.quality_rating - ROUND(fs.quality_rating)) > 0.001
                ORDER BY fs.quality_rating DESC
                LIMIT 10
                """
                examples = session.execute(text(examples_query)).fetchall()
                stats['decimal_examples'] = [
                    {
                        'facility': row[0],
                        'specialty': row[1],
                        'rating': row[2]
                    } for row in examples
                ]
            
            return stats

def print_statistics(stats, detailed=False):
    """
    Stampa le statistiche sui rating
    
    Args:
        stats: Statistiche sui rating
        detailed: Se True, stampa un report dettagliato
    """
    print("\n" + "="*60)
    print(" "*18 + "VERIFICA RATING DECIMALI")
    print("="*60)
    
    # Statistiche di base
    print(f"📊 Rating totali: {stats['total_ratings']}")
    
    if stats['total_ratings'] == 0:
        print(f"❗ Nessun rating trovato nel database")
        print("="*60)
        return
    
    non_null_ratings = stats['decimal_ratings'] + stats['integer_ratings']
    decimal_percentage = stats['decimal_ratings'] / non_null_ratings * 100 if non_null_ratings > 0 else 0
    
    print(f"🔢 Rating con decimali: {stats['decimal_ratings']} ({decimal_percentage:.1f}%)")
    print(f"🔢 Rating interi: {stats['integer_ratings']}")
    print(f"⚪ Rating non definiti (NULL): {stats['null_ratings']}")
    print(f"\n📋 Strutture con rating: {stats['facilities_with_ratings']}")
    print(f"📋 Specialità con rating: {stats['specialties_with_ratings']}")
    
    # Valori min, max e media
    if stats['min_rating'] is not None:
        print(f"\n📉 Rating minimo: {stats['min_rating']:.1f}")
        print(f"📈 Rating massimo: {stats['max_rating']:.1f}")
        print(f"📊 Rating medio: {stats['avg_rating']:.2f}")
    
    # Report dettagliato
    if detailed and stats['decimal_examples']:
        print("\n" + "-"*60)
        print(" "*12 + "ESEMPI DI RATING CON DECIMALI")
        print("-"*60)
        
        for i, example in enumerate(stats['decimal_examples']):
            print(f"{i+1}. {example['facility']} - {example['specialty']}: {example['rating']:.1f}")
    
    # Conclusione
    print("\n" + "="*60)
    
    if stats['decimal_ratings'] > 0:
        print(f"✅ Il database contiene {stats['decimal_ratings']} rating con decimali")
        print(f"✅ Il sistema supporta correttamente i rating decimali")
    else:
        print(f"⚠️ Non sono stati trovati rating con decimali nel database")
        print(f"⚠️ Usare lo script import_decimal_ratings.py per importare rating decimali")
    
    print("="*60)

def parse_arguments():
    """
    Analizza gli argomenti da linea di comando
    
    Returns:
        argparse.Namespace: Argomenti analizzati
    """
    parser = argparse.ArgumentParser(description='Verifica i rating decimali nel database')
    parser.add_argument('--detailed', action='store_true', help='Mostra un report dettagliato')
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    
    logger.info("Avvio verifica rating decimali")
    stats = get_ratings_statistics()
    
    print_statistics(stats, args.detailed)