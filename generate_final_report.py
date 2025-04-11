#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script per generare un report finale delle correzioni applicate al database

Questo script raccoglie le informazioni dai report di correzione già generati
e crea un report finale con statistiche complessive.
"""

import re
import glob

# Ottieni tutti i file di report di correzione
report_files = glob.glob("*batch_correction.txt")

# Inizializziamo le statistiche totali
total_stats = {
    'total_facilities_checked': 0,
    'total_facilities_updated': 0,
    'total_specialties_checked': 0,
    'total_specialties_updated': 0,
    'total_specialties_added': 0,
    'total_already_correct': 0,
    'updated_facilities': []
}

# Pattern per estrarre le statistiche dai report
for report_file in report_files:
    print(f"Elaborazione report: {report_file}")
    with open(report_file, 'r', encoding='utf-8') as f:
        content = f.read()
        
        # Estrai le statistiche generali
        match = re.search(r"Strutture verificate: (\d+)", content)
        if match:
            total_stats['total_facilities_checked'] += int(match.group(1))
        
        match = re.search(r"Strutture aggiornate: (\d+)", content)
        if match:
            total_stats['total_facilities_updated'] += int(match.group(1))
        
        match = re.search(r"Specialità verificate: (\d+)", content)
        if match:
            total_stats['total_specialties_checked'] += int(match.group(1))
        
        match = re.search(r"Specialità già corrette: (\d+)", content)
        if match:
            total_stats['total_already_correct'] += int(match.group(1))
        
        match = re.search(r"Specialità aggiornate: (\d+)", content)
        if match:
            total_stats['total_specialties_updated'] += int(match.group(1))
        
        match = re.search(r"Specialità aggiunte: (\d+)", content)
        if match:
            total_stats['total_specialties_added'] += int(match.group(1))
        
        # Estrai i dettagli delle strutture aggiornate
        blocks = re.findall(r"Struttura: (.*?)---------------------------", content, re.DOTALL)
        for block in blocks:
            lines = block.strip().split('\n')
            if len(lines) >= 3:
                facility_name = lines[0].strip()
                city = lines[1].replace("Città: ", "").strip() if lines[1].startswith("Città:") else ""
                
                # Aggiungi alla lista di strutture aggiornate
                total_stats['updated_facilities'].append(f"{facility_name} ({city})")

# Genera il report finale
with open("final_correction_report.txt", 'w', encoding='utf-8') as f:
    f.write("============================================\n")
    f.write("       REPORT FINALE CORREZIONI DATABASE     \n")
    f.write("============================================\n\n")
    
    f.write("STATISTICHE COMPLESSIVE\n")
    f.write("----------------------\n")
    f.write(f"Strutture verificate: {total_stats['total_facilities_checked']}\n")
    
    if total_stats['total_facilities_checked'] > 0:
        percentage = (total_stats['total_facilities_updated'] / total_stats['total_facilities_checked']) * 100
        f.write(f"Strutture aggiornate: {total_stats['total_facilities_updated']} ({percentage:.2f}%)\n")
    
    f.write(f"Specialità verificate: {total_stats['total_specialties_checked']}\n")
    
    if total_stats['total_specialties_checked'] > 0:
        already_correct_percentage = (total_stats['total_already_correct'] / total_stats['total_specialties_checked']) * 100
        updated_percentage = (total_stats['total_specialties_updated'] / total_stats['total_specialties_checked']) * 100
        added_percentage = (total_stats['total_specialties_added'] / total_stats['total_specialties_checked']) * 100
        
        f.write(f"Specialità già corrette: {total_stats['total_already_correct']} ({already_correct_percentage:.2f}%)\n")
        f.write(f"Specialità aggiornate: {total_stats['total_specialties_updated']} ({updated_percentage:.2f}%)\n")
        f.write(f"Specialità aggiunte: {total_stats['total_specialties_added']} ({added_percentage:.2f}%)\n")
    
    f.write("\n")
    f.write("STRUTTURE AGGIORNATE\n")
    f.write("-------------------\n")
    
    # Rimuovi duplicati ordinando alfabeticamente
    unique_facilities = sorted(set(total_stats['updated_facilities']))
    for i, facility in enumerate(unique_facilities, 1):
        f.write(f"{i}. {facility}\n")
    
    f.write("\n============================================\n")

print(f"Report finale generato: final_correction_report.txt")

# Mostra un riassunto a schermo
print("\n============================================")
print("       RIASSUNTO FINALE CORREZIONI         ")
print("============================================\n")
print(f"Strutture verificate: {total_stats['total_facilities_checked']}")
print(f"Strutture aggiornate: {total_stats['total_facilities_updated']} ({(total_stats['total_facilities_updated']/total_stats['total_facilities_checked']*100):.2f}%)")
print(f"Specialità verificate: {total_stats['total_specialties_checked']}")
print(f"Specialità già corrette: {total_stats['total_already_correct']} ({(total_stats['total_already_correct']/total_stats['total_specialties_checked']*100):.2f}%)")
print(f"Specialità aggiornate: {total_stats['total_specialties_updated']} ({(total_stats['total_specialties_updated']/total_stats['total_specialties_checked']*100):.2f}%)")
print(f"Specialità aggiunte: {total_stats['total_specialties_added']} ({(total_stats['total_specialties_added']/total_stats['total_specialties_checked']*100):.2f}%)")
print(f"\nTotale strutture uniche aggiornate: {len(unique_facilities)}")
print("\n============================================")