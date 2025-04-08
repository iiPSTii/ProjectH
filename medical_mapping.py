"""
Medical condition to specialty mapping for FindMyCure Italia.
This module provides functionality to map common medical conditions and symptoms
to appropriate medical specialties, enabling more intuitive search.
"""

# Map of common medical conditions/symptoms to appropriate specialties
# Format: 'condition_keyword': ['specialty1', 'specialty2', ...]
CONDITION_TO_SPECIALTY_MAP = {
    # General body parts - map these to likely specialties
    'piede': ['Ortopedia', 'Fisioterapia', 'Podologia'],
    'gamba': ['Ortopedia', 'Fisioterapia', 'Chirurgia Vascolare'],
    'mano': ['Ortopedia', 'Fisioterapia', 'Chirurgia della Mano'],
    'braccio': ['Ortopedia', 'Fisioterapia'],
    'petto': ['Cardiologia', 'Pneumologia', 'Medicina Interna'],
    'torace': ['Cardiologia', 'Pneumologia', 'Medicina Interna'],
    'gambe': ['Ortopedia', 'Fisioterapia', 'Chirurgia Vascolare'],
    'seno': ['Senologia', 'Ginecologia', 'Oncologia'],
    'testa': ['Neurologia', 'Otorinolaringoiatria', 'Oculistica'],
    'viso': ['Dermatologia', 'Chirurgia Plastica', 'Otorinolaringoiatria'],
    'piedi': ['Ortopedia', 'Fisioterapia', 'Podologia'],
    'caviglia': ['Ortopedia', 'Fisioterapia'],
    'dita': ['Ortopedia', 'Fisioterapia', 'Chirurgia della Mano'],
    'addome': ['Gastroenterologia', 'Chirurgia Generale', 'Medicina Interna'],
    'gola': ['Otorinolaringoiatria', 'Gastroenterologia'],
    
    # General symptoms - map to likely specialties
    'male': ['Medicina Interna', 'Medicina Generale'],
    'malessere': ['Medicina Interna', 'Medicina Generale'],
    'dolore': ['Medicina Interna', 'Terapia del Dolore', 'Fisioterapia'],
    'dolori': ['Medicina Interna', 'Terapia del Dolore', 'Ortopedia', 'Fisioterapia'],
    'febbre': ['Medicina Interna', 'Malattie Infettive'],
    'infiammazione': ['Medicina Interna', 'Reumatologia', 'Immunologia'],
    'gonfiore': ['Medicina Interna', 'Angiologia', 'Reumatologia'],
    'formicolio': ['Neurologia', 'Ortopedia'],
    'bruciore': ['Dermatologia', 'Gastroenterologia'],
    'prurito': ['Dermatologia', 'Allergologia'],
    'stanchezza': ['Medicina Interna', 'Ematologia', 'Endocrinologia'],
    'dolore al petto': ['Cardiologia', 'Pneumologia', 'Chirurgia Toracica'],
    'dolori al petto': ['Cardiologia', 'Pneumologia', 'Chirurgia Toracica'],
    
    # Specific body-part combinations
    'male alla schiena': ['Ortopedia', 'Fisioterapia', 'Neurologia'],
    'male al collo': ['Ortopedia', 'Fisioterapia', 'Neurologia'],
    'male alla testa': ['Neurologia', 'Medicina Interna'],
    'male alle gambe': ['Ortopedia', 'Angiologia', 'Fisioterapia'],
    'male ai piedi': ['Ortopedia', 'Podologia', 'Fisioterapia'],
    'dolore alla schiena': ['Ortopedia', 'Fisioterapia', 'Neurologia'],
    'dolore al ginocchio': ['Ortopedia', 'Fisioterapia'],
    'dolore al collo': ['Ortopedia', 'Fisioterapia', 'Neurologia'],
    'dolore addominale': ['Gastroenterologia', 'Medicina Interna', 'Chirurgia Generale'],
    'male al piede': ['Ortopedia', 'Podologia', 'Fisioterapia'],
    'male alla gamba': ['Ortopedia', 'Angiologia', 'Fisioterapia'],
    'male al braccio': ['Ortopedia', 'Fisioterapia'],
    'mal di testa': ['Neurologia', 'Medicina Interna'],
    'mal di gola': ['Otorinolaringoiatria', 'Medicina Interna'],
    'mal di schiena': ['Ortopedia', 'Fisioterapia', 'Neurologia'],
    'mal di stomaco': ['Gastroenterologia', 'Medicina Interna'],
    'mal di denti': ['Odontoiatria'],
    'mal di orecchio': ['Otorinolaringoiatria'],
    # Back, joint and bone problems -> Orthopedics, Rheumatology, Physiotherapy
    'schiena': ['Ortopedia', 'Reumatologia', 'Fisioterapia', 'Neurologia'],
    'lombare': ['Ortopedia', 'Fisioterapia', 'Neurologia'],
    'cervicale': ['Ortopedia', 'Fisioterapia', 'Neurologia'],
    'collo': ['Ortopedia', 'Otorinolaringoiatria', 'Fisioterapia'],
    'artrite': ['Reumatologia', 'Ortopedia', 'Medicina Interna'],
    'artrosi': ['Ortopedia', 'Reumatologia', 'Fisioterapia'],
    'articolazione': ['Ortopedia', 'Reumatologia', 'Fisioterapia'],
    'ginocchio': ['Ortopedia', 'Fisioterapia', 'Medicina dello Sport'],
    'anca': ['Ortopedia', 'Fisioterapia'],
    'spalla': ['Ortopedia', 'Fisioterapia'],
    'frattura': ['Ortopedia', 'Traumatologia'],
    'osteoporosi': ['Ortopedia', 'Endocrinologia', 'Reumatologia'],
    'tendinite': ['Ortopedia', 'Medicina dello Sport', 'Fisioterapia'],
    
    # Digestive system -> Gastroenterology
    'stomaco': ['Gastroenterologia', 'Medicina Interna'],
    'intestino': ['Gastroenterologia', 'Chirurgia Generale'],
    'colon': ['Gastroenterologia', 'Chirurgia Generale', 'Oncologia'],
    'fegato': ['Gastroenterologia', 'Epatologia', 'Medicina Interna'],
    'digestione': ['Gastroenterologia', 'Nutrizione'],
    'colite': ['Gastroenterologia', 'Medicina Interna'],
    'gastrite': ['Gastroenterologia'],
    'reflusso': ['Gastroenterologia', 'Medicina Interna'],
    'ulcera': ['Gastroenterologia', 'Chirurgia Generale'],
    'emorroidi': ['Proctologia', 'Chirurgia Generale'],
    'diarrea': ['Gastroenterologia', 'Medicina Interna', 'Malattie Infettive'],
    'stipsi': ['Gastroenterologia', 'Medicina Interna'],
    'celiachia': ['Gastroenterologia', 'Allergologia', 'Nutrizione'],
    
    # Heart and circulatory system -> Cardiology
    'cuore': ['Cardiologia', 'Medicina Interna', 'Cardiochirurgia'],
    'pressione': ['Cardiologia', 'Medicina Interna', 'Nefrologia'],
    'ipertensione': ['Cardiologia', 'Medicina Interna', 'Nefrologia'],
    'colesterolo': ['Cardiologia', 'Medicina Interna', 'Endocrinologia'],
    'vene': ['Angiologia', 'Chirurgia Vascolare'],
    'varici': ['Angiologia', 'Chirurgia Vascolare'],
    'trombosi': ['Angiologia', 'Chirurgia Vascolare', 'Ematologia'],
    'aritmia': ['Cardiologia', 'Elettrofisiologia'],
    'infarto': ['Cardiologia', 'Cardiochirurgia', 'Medicina d\'Urgenza'],
    'ictus': ['Neurologia', 'Cardiologia', 'Medicina d\'Urgenza'],
    
    # Respiratory system -> Pulmonology, ENT
    'polmoni': ['Pneumologia', 'Medicina Interna'],
    'respiro': ['Pneumologia', 'Cardiologia', 'Allergologia'],
    'asma': ['Pneumologia', 'Allergologia', 'Medicina Interna'],
    'bronchite': ['Pneumologia', 'Medicina Interna'],
    'tosse': ['Pneumologia', 'Otorinolaringoiatria', 'Medicina Interna', 'Allergologia'],
    'sinusite': ['Otorinolaringoiatria', 'Allergologia'],
    'rinite': ['Otorinolaringoiatria', 'Allergologia'],
    'naso': ['Otorinolaringoiatria', 'Chirurgia Plastica'],
    'gola': ['Otorinolaringoiatria', 'Gastroenterologia'],
    'orecchio': ['Otorinolaringoiatria', 'Audiologia'],
    'udito': ['Otorinolaringoiatria', 'Audiologia'],
    'allergia': ['Allergologia', 'Immunologia', 'Dermatologia'],
    
    # Urinary system -> Urology, Nephrology
    'reni': ['Nefrologia', 'Urologia'],
    'vescica': ['Urologia', 'Ginecologia'],
    'urina': ['Urologia', 'Nefrologia'],
    'prostata': ['Urologia', 'Andrologia'],
    'incontinenza': ['Urologia', 'Ginecologia', 'Neurologia'],
    'calcoli': ['Urologia', 'Nefrologia'],
    
    # Reproductive system -> Gynecology, Urology
    'fertilità': ['Ginecologia', 'Andrologia', 'Endocrinologia'],
    'gravidanza': ['Ginecologia', 'Ostetricia'],
    'menopausa': ['Ginecologia', 'Endocrinologia'],
    'ciclo': ['Ginecologia', 'Endocrinologia'],
    'ovaia': ['Ginecologia'],
    'utero': ['Ginecologia'],
    'pap': ['Ginecologia'],
    'mammografia': ['Senologia', 'Radiologia'],
    'seno': ['Senologia', 'Chirurgia Plastica', 'Ginecologia'],
    'impotenza': ['Andrologia', 'Urologia', 'Psicologia'],
    'testosterone': ['Andrologia', 'Endocrinologia'],
    
    # Skin -> Dermatology
    'pelle': ['Dermatologia', 'Allergologia'],
    'acne': ['Dermatologia'],
    'eczema': ['Dermatologia', 'Allergologia'],
    'psoriasi': ['Dermatologia', 'Immunologia'],
    'melanoma': ['Dermatologia', 'Oncologia'],
    'neo': ['Dermatologia', 'Chirurgia Plastica'],
    'dermatite': ['Dermatologia', 'Allergologia'],
    
    # Eyes -> Ophthalmology
    'occhi': ['Oculistica', 'Neurologia'],
    'vista': ['Oculistica', 'Neurologia'],
    'cataratta': ['Oculistica'],
    'glaucoma': ['Oculistica'],
    'miopia': ['Oculistica'],
    'retina': ['Oculistica'],
    
    # Brain and nervous system -> Neurology
    'testa': ['Neurologia', 'Otorinolaringoiatria'],
    'cefalea': ['Neurologia', 'Medicina Interna'],
    'emicrania': ['Neurologia'],
    'memoria': ['Neurologia', 'Geriatria', 'Psichiatria'],
    'alzheimer': ['Neurologia', 'Geriatria'],
    'parkinson': ['Neurologia', 'Geriatria'],
    'epilessia': ['Neurologia'],
    'sclerosi': ['Neurologia', 'Immunologia'],
    'atassia': ['Neurologia'],
    'paralisi': ['Neurologia', 'Fisioterapia', 'Neurochirurgia'],
    'neuropatia': ['Neurologia', 'Diabetologia'],
    'tremori': ['Neurologia', 'Geriatria'],
    'vertigini': ['Neurologia', 'Otorinolaringoiatria'],
    
    # Mental health -> Psychiatry, Psychology
    'ansia': ['Psichiatria', 'Psicologia'],
    'depressione': ['Psichiatria', 'Psicologia'],
    'disturbo': ['Psichiatria', 'Psicologia'],
    'insonnia': ['Neurologia', 'Psichiatria', 'Medicina del Sonno'],
    'stress': ['Psichiatria', 'Psicologia'],
    'alimentare': ['Nutrizione', 'Psichiatria', 'Endocrinologia'],
    'anoressia': ['Psichiatria', 'Nutrizione'],
    'bulimia': ['Psichiatria', 'Nutrizione'],
    'adhd': ['Neuropsichiatria', 'Psicologia'],
    'autismo': ['Neuropsichiatria', 'Psicologia'],
    
    # Endocrine system -> Endocrinology
    'diabete': ['Diabetologia', 'Endocrinologia', 'Medicina Interna'],
    'tiroide': ['Endocrinologia', 'Medicina Nucleare'],
    'ormoni': ['Endocrinologia', 'Ginecologia', 'Andrologia'],
    'obesità': ['Endocrinologia', 'Nutrizione', 'Medicina Interna'],
    'metabolismo': ['Endocrinologia', 'Nutrizione'],
    
    # Immune system -> Immunology, Rheumatology
    'immunità': ['Immunologia', 'Allergologia', 'Reumatologia'],
    'lupus': ['Reumatologia', 'Immunologia', 'Dermatologia'],
    'autoimmune': ['Immunologia', 'Reumatologia'],
    
    # Dental -> Dentistry
    'denti': ['Odontoiatria'],
    'gengive': ['Odontoiatria', 'Parodontologia'],
    'carie': ['Odontoiatria'],
    
    # Cancer -> Oncology
    'cancro': ['Oncologia', 'Medicina Interna'],
    'tumore': ['Oncologia', 'Chirurgia'],
    'chemioterapia': ['Oncologia'],
    'radioterapia': ['Radioterapia', 'Oncologia'],
    'metastasi': ['Oncologia'],
    'leucemia': ['Ematologia', 'Oncologia'],
    'linfoma': ['Ematologia', 'Oncologia'],
    
    # Blood -> Hematology
    'sangue': ['Ematologia', 'Medicina Interna'],
    'anemia': ['Ematologia', 'Medicina Interna'],
    'piastrine': ['Ematologia'],
    'coagulazione': ['Ematologia'],
    
    # Pediatrics
    'bambino': ['Pediatria'],
    'pediatria': ['Pediatria'],
    'crescita': ['Pediatria', 'Endocrinologia'],
    
    # General symptoms that could map to multiple specialties
    'dolore': ['Medicina Interna', 'Terapia del Dolore', 'Fisioterapia'],
    'febbre': ['Medicina Interna', 'Malattie Infettive'],
    'infezione': ['Malattie Infettive', 'Medicina Interna'],
    'perdita peso': ['Medicina Interna', 'Endocrinologia', 'Nutrizione', 'Gastroenterologia'],
    'stanchezza': ['Medicina Interna', 'Endocrinologia', 'Ematologia', 'Psichiatria'],
    'prevenzione': ['Medicina Preventiva', 'Medicina Interna'],
    'chirurgia': ['Chirurgia Generale'],
    'riabilitazione': ['Fisioterapia', 'Medicina Riabilitativa'],
    'medicina sportiva': ['Medicina dello Sport', 'Ortopedia', 'Fisioterapia'],
}

def map_query_to_specialties(query):
    """
    Map a user query to relevant medical specialties based on identified keywords.
    
    Args:
        query (str): The user's search query text
        
    Returns:
        list: List of specialty names that are relevant to the query
    """
    if not query:
        return []
    
    # Convert query to lowercase for case-insensitive matching
    query = query.lower()
    
    # First try multi-word exact matches (e.g., "dolore al petto")
    matching_specialties = set()
    
    # Try exact phrase matching first
    for keyword, specialties in CONDITION_TO_SPECIALTY_MAP.items():
        keyword_lower = keyword.lower()
        if len(keyword_lower.split()) > 1 and keyword_lower in query:
            # Add all mapped specialties for this condition
            for specialty in specialties:
                matching_specialties.add(specialty)
    
    # If no multi-word matches found, try single-word matching
    if not matching_specialties:
        for keyword, specialties in CONDITION_TO_SPECIALTY_MAP.items():
            keyword_lower = keyword.lower()
            # Only check single words to avoid false positives
            if keyword_lower in query.split() or (' ' + keyword_lower + ' ') in (' ' + query + ' '):
                # Add all mapped specialties for this condition
                for specialty in specialties:
                    matching_specialties.add(specialty)
    
    # If still no matches, try broader partial matching for important body parts
    if not matching_specialties:
        for keyword, specialties in CONDITION_TO_SPECIALTY_MAP.items():
            keyword_lower = keyword.lower()
            if keyword_lower in query:
                # Add all mapped specialties for this condition
                for specialty in specialties:
                    matching_specialties.add(specialty)
    
    # If we didn't find any matches but the query contains general terms like "male" (hurt/pain)
    # add some fallback general specialties
    if not matching_specialties and len(query.split()) > 1:
        if any(term in query for term in ['male', 'dolore', 'dolori', 'mal']):
            matching_specialties.update(['Medicina Interna', 'Medicina Generale', 'Ortopedia'])
    
    # Sort by relevance (this is a placeholder - ideally would sort based on query relevance)
    return list(matching_specialties)