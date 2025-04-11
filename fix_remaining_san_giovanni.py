#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script per correggere le valutazioni rimanenti per l'Ospedale San Giovanni di Dio di Gorizia

Questo script corregge le discrepanze rimanenti per l'Ospedale San Giovanni di Dio di Gorizia,
assicurando che i valori nel database corrispondano a quelli nel CSV.
"""

import logging
import sys
import os
from datetime import datetime

import sqlalchemy
from sqlalchemy.orm import Session

sys.path.append(".")
from app import app, db
from models import MedicalFacility, Specialty, FacilitySpecialty, DatabaseStatus

# Configurazione del logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_database_status(message):
    """Aggiorna lo stato del database"""
    with app.app_context():
        with Session(db.engine) as session:
            status = session.query(DatabaseStatus).first()
            if status:
                status.status = "updated"
                status.last_updated = datetime.now()
                status.message = message
                session.commit()
                logger.info(f"Stato database aggiornato: {message}")
            else:
                logger.warning("Stato database non trovato")

def fix_gorizia_ratings():
    """
    Corregge le valutazioni per l'Ospedale San Giovanni di Dio di Gorizia
    
    Returns:
        dict: Statistiche sugli aggiornamenti
    """
    # Valori corretti dal CSV
    correct_ratings = {
        'Cardiologia': 1.0,
        'Ortopedia': 4.0,
        'Oncologia': 5.0,
        'Neurologia': 3.0,
        'Urologia': 3.0,
        'Chirurgia Generale': 5.0,
        'Pediatria': 3.0,
        'Ginecologia': 2.0
    }
    
    stats = {
        'facility': 'Ospedale San Giovanni di Dio (Gorizia)',
        'specialties_checked': 0,
        'specialties_updated': 0,
        'updates': []
    }
    
    with app.app_context():
        try:
            with Session(db.engine) as session:
                # Ottieni la struttura
                facility = session.query(MedicalFacility).filter(
                    MedicalFacility.name == 'Ospedale San Giovanni di Dio',
                    MedicalFacility.city == 'Gorizia'
                ).first()
                
                if not facility:
                    logger.error("Struttura 'Ospedale San Giovanni di Dio' (Gorizia) non trovata")
                    return stats
                
                # Ottieni tutte le specialità e verifica/aggiorna i rating
                for specialty_name, correct_rating in correct_ratings.items():
                    stats['specialties_checked'] += 1
                    
                    # Ottieni la specialità
                    specialty = session.query(Specialty).filter(
                        Specialty.name == specialty_name
                    ).first()
                    
                    if not specialty:
                        logger.warning(f"Specialità '{specialty_name}' non trovata nel database")
                        continue
                    
                    # Ottieni il record facility-specialty
                    facility_specialty = session.query(FacilitySpecialty).filter(
                        FacilitySpecialty.facility_id == facility.id,
                        FacilitySpecialty.specialty_id == specialty.id
                    ).first()
                    
                    if not facility_specialty:
                        logger.info(f"Creazione nuovo record per '{specialty_name}'")
                        facility_specialty = FacilitySpecialty(
                            facility_id=facility.id,
                            specialty_id=specialty.id,
                            quality_rating=correct_rating
                        )
                        session.add(facility_specialty)
                        stats['specialties_updated'] += 1
                        stats['updates'].append(f"{specialty_name}: Nuovo -> {correct_rating}")
                    else:
                        current_rating = facility_specialty.quality_rating
                        if current_rating != correct_rating:
                            logger.info(f"Aggiornamento rating per '{specialty_name}': {current_rating} -> {correct_rating}")
                            facility_specialty.quality_rating = correct_rating
                            stats['specialties_updated'] += 1
                            stats['updates'].append(f"{specialty_name}: {current_rating} -> {correct_rating}")
                
                # Commit alla fine per evitare problemi di transazione
                session.commit()
                update_database_status(f"Aggiornate {stats['specialties_updated']} specialità per Ospedale San Giovanni di Dio (Gorizia)")
                
        except Exception as e:
            logger.error(f"Errore durante l'aggiornamento: {str(e)}")
            
    return stats

def print_stats(stats):
    """
    Stampa le statistiche degli aggiornamenti
    
    Args:
        stats: Statistiche
    """
    print("\n==============================================")
    print(f"Struttura: {stats['facility']}")
    print(f"Specialità controllate: {stats['specialties_checked']}")
    print(f"Specialità aggiornate: {stats['specialties_updated']}")
    
    if stats['updates']:
        print("\nAggiornamenti effettuati:")
        for update in stats['updates']:
            print(f"- {update}")
    else:
        print("\nNessun aggiornamento necessario. I dati erano già corretti.")
    
    print("==============================================\n")

if __name__ == "__main__":
    print("Correzione valutazioni per Ospedale San Giovanni di Dio (Gorizia)...")
    
    # Esegui la correzione
    stats = fix_gorizia_ratings()
    
    # Stampa le statistiche
    print_stats(stats)