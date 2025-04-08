"""
Medical professionals to specialty mapping for FindMyCure Italia.
This module provides functionality to map medical profession terms to specialties.
"""

# Map of medical profession terms to specialties
# Format: 'profession_term': ['specialty1', 'specialty2', ...]
PROFESSION_TO_SPECIALTY_MAP = {
    # Cardiology
    'cardiologo': ['Cardiologia'],
    'cardiologia': ['Cardiologia'],
    'cardiologa': ['Cardiologia'],
    
    # Oncology
    'oncologo': ['Oncologia'],
    'oncologia': ['Oncologia'],
    'oncologa': ['Oncologia'],
    
    # Orthopedics
    'ortopedico': ['Ortopedia'],
    'ortopedia': ['Ortopedia'],
    'ortopedica': ['Ortopedia'],
    
    # Neurology
    'neurologo': ['Neurologia'],
    'neurologia': ['Neurologia'],
    'neurologa': ['Neurologia'],
    
    # Dermatology
    'dermatologo': ['Dermatologia'],
    'dermatologia': ['Dermatologia'],
    'dermatologa': ['Dermatologia'],
    
    # Gastroenterology
    'gastroenterologo': ['Gastroenterologia'],
    'gastroenterologia': ['Gastroenterologia'],
    'gastroenterologa': ['Gastroenterologia'],
    
    # Gynecology
    'ginecologo': ['Ginecologia'],
    'ginecologia': ['Ginecologia'],
    'ginecologa': ['Ginecologia'],
    
    # Urology
    'urologo': ['Urologia'],
    'urologia': ['Urologia'],
    'urologa': ['Urologia'],
    
    # Ophthalmology
    'oculista': ['Oculistica'],
    'oculistica': ['Oculistica'],
    'oftalmologo': ['Oculistica'],
    'oftalmologa': ['Oculistica'],
    'oftalmologia': ['Oculistica'],
    
    # Otolaryngology (ENT)
    'otorinolaringoiatra': ['Otorinolaringoiatria'],
    'otorinolaringoiatria': ['Otorinolaringoiatria'],
    'otorino': ['Otorinolaringoiatria'],
    
    # Pediatrics
    'pediatra': ['Pediatria'],
    'pediatria': ['Pediatria'],
    
    # Psychiatry
    'psichiatra': ['Psichiatria'],
    'psichiatria': ['Psichiatria'],
    
    # Psychology
    'psicologo': ['Psicologia'],
    'psicologia': ['Psicologia'],
    'psicologa': ['Psicologia'],
    
    # Dentistry
    'dentista': ['Odontoiatria'],
    'odontoiatra': ['Odontoiatria'],
    'odontoiatria': ['Odontoiatria'],
    
    # Endocrinology
    'endocrinologo': ['Endocrinologia'],
    'endocrinologia': ['Endocrinologia'],
    'endocrinologa': ['Endocrinologia'],
    
    # Radiology
    'radiologo': ['Radiologia'],
    'radiologia': ['Radiologia'],
    'radiologa': ['Radiologia'],
    
    # Rheumatology
    'reumatologo': ['Reumatologia'],
    'reumatologia': ['Reumatologia'],
    'reumatologa': ['Reumatologia'],
    
    # Pulmonology
    'pneumologo': ['Pneumologia'],
    'pneumologia': ['Pneumologia'],
    'pneumologa': ['Pneumologia'],
    
    # Hematology
    'ematologo': ['Ematologia'],
    'ematologia': ['Ematologia'],
    'ematologa': ['Ematologia'],
    
    # Nephrology
    'nefrologo': ['Nefrologia'],
    'nefrologia': ['Nefrologia'],
    'nefrologa': ['Nefrologia'],
    
    # Allergology
    'allergologo': ['Allergologia'],
    'allergologia': ['Allergologia'],
    'allergologa': ['Allergologia'],
    
    # Internal Medicine
    'internista': ['Medicina Interna'],
    'medicina interna': ['Medicina Interna'],
    
    # General Surgery
    'chirurgo': ['Chirurgia Generale'],
    'chirurgia': ['Chirurgia Generale'],
    'chirurga': ['Chirurgia Generale'],
    
    # Vascular Surgery
    'chirurgo vascolare': ['Chirurgia Vascolare'],
    'chirurgia vascolare': ['Chirurgia Vascolare'],
    
    # Plastic Surgery
    'chirurgo plastico': ['Chirurgia Plastica'],
    'chirurgia plastica': ['Chirurgia Plastica'],
    
    # Sports Medicine
    'medico sportivo': ['Medicina dello Sport'],
    'medicina sportiva': ['Medicina dello Sport'],
    'medicina dello sport': ['Medicina dello Sport'],
    
    # Physical Therapy
    'fisioterapista': ['Fisioterapia'],
    'fisioterapia': ['Fisioterapia'],
    
    # Nutrition
    'nutrizionista': ['Nutrizione'],
    'nutrizione': ['Nutrizione'],
    'dietista': ['Nutrizione'],
    'dietologia': ['Nutrizione'],
    
    # General terms
    'medico': ['Medicina Generale', 'Medicina Interna'],
    'dottore': ['Medicina Generale', 'Medicina Interna'],
    'specialista': ['Medicina Generale'],
    'specialistica': ['Medicina Generale'],
    'ambulatorio': ['Medicina Generale'],
    'visita': ['Medicina Generale'],
}

def map_profession_to_specialties(query):
    """
    Map a medical profession query to relevant specialties.
    
    Args:
        query (str): The user's search query text
        
    Returns:
        list: List of specialty names that are relevant to the query
    """
    if not query:
        return []
    
    # Convert query to lowercase for case-insensitive matching
    query_lower = query.lower()
    query_words = query_lower.split()
    
    # Find matching professions in the query
    matching_specialties = set()
    
    # First, try to match full query
    if query_lower in PROFESSION_TO_SPECIALTY_MAP:
        for specialty in PROFESSION_TO_SPECIALTY_MAP[query_lower]:
            matching_specialties.add(specialty)
    
    # If no match found, try individual words
    if not matching_specialties:
        for word in query_words:
            if word in PROFESSION_TO_SPECIALTY_MAP:
                for specialty in PROFESSION_TO_SPECIALTY_MAP[word]:
                    matching_specialties.add(specialty)
    
    return list(matching_specialties)