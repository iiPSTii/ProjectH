"""
Specialty macrocategory mapping for FindMyCure Italia.
This module provides mappings between the dropdown specialty options and the 
actual specialty names stored in the database.
"""

# Map of dropdown specialty options to database specialty names
# Format: 'dropdown_specialty': ['db_specialty1', 'db_specialty2', ...]
SPECIALTY_MAPPING = {
    # Exact matches plus related specialties to ensure results
    'Allergologia': ['Allergologia', 'Medicina Interna', 'Pneumologia', 'Dermatologia', 'Immunologia'],
    'Cardiologia': ['Cardiologia', 'Medicina Interna', 'Chirurgia Cardiaca', 'Medicina Generale'],
    'Chirurgia': ['Chirurgia', 'Chirurgia Generale', 'Ortopedia e Traumatologia', 'Chirurgia Plastica', 'Chirurgia Vascolare', 'Chirurgia Toracica', 'Chirurgia Pediatrica'],
    'Dermatologia': ['Dermatologia', 'Medicina Interna', 'Medicina Estetica', 'Allergologia'],
    'Diagnostica': ['Diagnostica per Immagini', 'Radiologia', 'Medicina Nucleare', 'Ecografia', 'Laboratorio Analisi'],
    'Ematologia': ['Ematologia', 'Oncologia', 'Medicina Interna', 'Immunologia'],
    'Endocrinologia': ['Endocrinologia', 'Medicina Interna', 'Diabetologia', 'Medicina Generale'],
    'Fertilità': ['Fertilità', 'Ginecologia', 'Ginecologia e Ostetricia', 'Urologia', 'Andrologia'],
    'Gastroenterologia': ['Gastroenterologia', 'Medicina Interna', 'Epatologia', 'Chirurgia Generale', 'Medicina Generale'],
    'Ginecologia': ['Ginecologia', 'Ginecologia e Ostetricia', 'Ostetricia', 'Senologia'],
    'Malattie Infettive': ['Malattie Infettive', 'Virologia', 'Medicina Tropicale', 'Medicina Interna', 'Medicina Generale'],
    'Medicina Interna': ['Medicina Interna', 'Medicina Generale', 'Geriatria'],
    'Medicina d\'Urgenza': ['Pronto Soccorso', 'Urgenza', 'Medicina d\'Urgenza', 'Emergenza', 'Rianimazione', 'Terapia Intensiva'],
    'Medicina dello Sport': ['Medicina dello Sport', 'Medicina Sportiva', 'Ortopedia', 'Fisioterapia', 'Riabilitazione'],
    'Medicina Generale': ['Medicina Generale', 'Medicina Interna', 'Medicina di Base'],
    'Neurologia': ['Neurologia', 'Neurochirurgia', 'Medicina Interna', 'Neurofisiologia'],
    'Oculistica': ['Oculistica', 'Oftalmologia', 'Chirurgia Oculistica'],
    'Oncologia': ['Oncologia', 'Ematologia', 'Radioterapia', 'Chemioterapia', 'Chirurgia Oncologica'],
    'Ortopedia': ['Ortopedia', 'Ortopedia e Traumatologia', 'Traumatologia', 'Fisioterapia', 'Riabilitazione'],
    'Otorinolaringoiatria': ['Otorinolaringoiatria', 'ORL', 'Audiologia', 'Foniatria'],
    'Pediatria': ['Pediatria', 'Neonatologia', 'Chirurgia Pediatrica', 'Neuropsichiatria Infantile'],
    'Pneumologia': ['Pneumologia', 'Medicina Interna', 'Chirurgia Toracica', 'Medicina Generale'],
    'Psichiatria': ['Psichiatria', 'Psicologia', 'Neurologia', 'Neuropsichiatria'],
    'Radiologia': ['Radiologia', 'Diagnostica per Immagini', 'Medicina Nucleare', 'Radioterapia'],
    'Reumatologia': ['Reumatologia', 'Medicina Interna', 'Ortopedia', 'Immunologia', 'Medicina Generale'],
    'Riabilitazione': ['Riabilitazione', 'Fisioterapia', 'Medicina dello Sport', 'Medicina Sportiva', 'Ortopedia', 'Neurologia'],
    'Urologia': ['Urologia', 'Chirurgia Generale', 'Andrologia', 'Nefrologia'],
}

# For reverse lookup - map database specialty names to dropdown options
REVERSE_SPECIALTY_MAPPING = {}
for dropdown_name, db_names in SPECIALTY_MAPPING.items():
    for db_name in db_names:
        if db_name not in REVERSE_SPECIALTY_MAPPING:
            REVERSE_SPECIALTY_MAPPING[db_name] = []
        REVERSE_SPECIALTY_MAPPING[db_name].append(dropdown_name)

def get_equivalent_specialties(specialty_name):
    """
    Get all database specialty names that should be included when searching
    for the given specialty dropdown option.
    
    Args:
        specialty_name (str): The specialty name selected from dropdown
        
    Returns:
        list: List of equivalent specialty names in the database
    """
    if not specialty_name:
        return []
    
    # Try to get mapped specialties
    return SPECIALTY_MAPPING.get(specialty_name, [specialty_name])

def get_dropdown_specialties(db_specialty_name):
    """
    Get all dropdown specialty options that include the given database specialty.
    
    Args:
        db_specialty_name (str): The specialty name as stored in database
        
    Returns:
        list: List of dropdown specialty options that include this specialty
    """
    if not db_specialty_name:
        return []
    
    # Try to get mapped dropdown options
    return REVERSE_SPECIALTY_MAPPING.get(db_specialty_name, [db_specialty_name])