"""
Location mapping for FindMyCure Italia.
This module provides functionality to map Italian cities to their respective regions,
enabling more intuitive search results based on location references.
"""

# Map of Italian cities to their regions (including smaller municipalities)
# Format: 'city_name': 'region_name'
CITY_TO_REGION_MAP = {
    # Lombardia
    'milano': 'Lombardia',
    'bergamo': 'Lombardia',
    'brescia': 'Lombardia',
    'como': 'Lombardia',
    'cremona': 'Lombardia',
    'lecco': 'Lombardia',
    'lodi': 'Lombardia',
    'mantova': 'Lombardia',
    'monza': 'Lombardia',
    'pavia': 'Lombardia',
    'sondrio': 'Lombardia',
    'varese': 'Lombardia',
    'busto arsizio': 'Lombardia',
    'cinisello balsamo': 'Lombardia',
    'legnano': 'Lombardia',
    'rho': 'Lombardia',
    'cologno monzese': 'Lombardia',
    'paderno dugnano': 'Lombardia',
    'seregno': 'Lombardia',
    'cesano maderno': 'Lombardia',
    'segrate': 'Lombardia',
    
    # Lazio
    'roma': 'Lazio',
    'frosinone': 'Lazio',
    'latina': 'Lazio',
    'rieti': 'Lazio',
    'viterbo': 'Lazio',
    'guidonia': 'Lazio',
    'fiumicino': 'Lazio',
    'civitavecchia': 'Lazio',
    'velletri': 'Lazio',
    'anzio': 'Lazio',
    'nettuno': 'Lazio',
    'formia': 'Lazio',
    'ostia': 'Lazio',
    'cerveteri': 'Lazio',
    
    # Campania
    'napoli': 'Campania',
    'avellino': 'Campania',
    'benevento': 'Campania',
    'caserta': 'Campania',
    'salerno': 'Campania',
    'acerra': 'Campania',
    'afragola': 'Campania', 
    'agropoli': 'Campania',
    'amalfi': 'Campania',
    'angri': 'Campania',
    'ariano irpino': 'Campania',
    'arzano': 'Campania',
    'atripalda': 'Campania',
    'aversa': 'Campania',
    'bacoli': 'Campania',
    'battipaglia': 'Campania',
    'capua': 'Campania',
    'castellammare di stabia': 'Campania',
    'cava de tirreni': 'Campania',
    'ercolano': 'Campania',
    'giugliano in campania': 'Campania',
    'ischia': 'Campania',
    'marcianise': 'Campania',
    'marano di napoli': 'Campania',
    'mercato san severino': 'Campania',
    'nocera inferiore': 'Campania',
    'nola': 'Campania',
    'pagani': 'Campania',
    'piano di sorrento': 'Campania',
    'pompei': 'Campania',
    'portici': 'Campania',
    'positano': 'Campania',
    'pozzuoli': 'Campania',
    'ravello': 'Campania',
    'sant\'agnello': 'Campania',
    'sant\'anastasia': 'Campania',
    'scafati': 'Campania',
    'sorrento': 'Campania',
    'torre annunziata': 'Campania',
    'torre del greco': 'Campania',
    'vico equense': 'Campania',
    'volla': 'Campania',
    
    # Sicilia
    'palermo': 'Sicilia',
    'agrigento': 'Sicilia',
    'caltanissetta': 'Sicilia',
    'catania': 'Sicilia',
    'enna': 'Sicilia',
    'messina': 'Sicilia',
    'ragusa': 'Sicilia',
    'siracusa': 'Sicilia',
    'trapani': 'Sicilia',
    
    # Veneto
    'venezia': 'Veneto',
    'belluno': 'Veneto',
    'padova': 'Veneto',
    'rovigo': 'Veneto',
    'treviso': 'Veneto',
    'verona': 'Veneto',
    'vicenza': 'Veneto',
    'abano terme': 'Veneto',
    'adria': 'Veneto',
    'albignasego': 'Veneto',
    'arzignano': 'Veneto',
    'asolo': 'Veneto',
    'bassano del grappa': 'Veneto',
    'caorle': 'Veneto',
    'castelfranco veneto': 'Veneto',
    'chioggia': 'Veneto',
    'cittadella': 'Veneto',
    'conegliano': 'Veneto',
    'cortina d\'ampezzo': 'Veneto',
    'dolo': 'Veneto',
    'este': 'Veneto',
    'feltre': 'Veneto',
    'jesolo': 'Veneto',
    'legnago': 'Veneto',
    'mestre': 'Veneto',
    'mirano': 'Veneto',
    'montebelluna': 'Veneto',
    'monselice': 'Veneto',
    'negrar': 'Veneto',
    'noale': 'Veneto',
    'oderzo': 'Veneto',
    'peschiera del garda': 'Veneto',
    'piove di sacco': 'Veneto',
    'portogruaro': 'Veneto',
    'san donà di piave': 'Veneto',
    'schio': 'Veneto',
    'spinea': 'Veneto',
    'thiene': 'Veneto',
    'villafranca di verona': 'Veneto',
    'vittorio veneto': 'Veneto',
    
    # Piemonte
    'torino': 'Piemonte',
    'alessandria': 'Piemonte',
    'asti': 'Piemonte',
    'biella': 'Piemonte',
    'cuneo': 'Piemonte',
    'novara': 'Piemonte',
    'verbania': 'Piemonte',
    'vercelli': 'Piemonte',
    
    # Emilia-Romagna
    'bologna': 'Emilia-Romagna',
    'ferrara': 'Emilia-Romagna',
    'forlì': 'Emilia-Romagna',
    'cesena': 'Emilia-Romagna',
    'modena': 'Emilia-Romagna',
    'parma': 'Emilia-Romagna',
    'piacenza': 'Emilia-Romagna',
    'ravenna': 'Emilia-Romagna',
    'reggio emilia': 'Emilia-Romagna',
    'rimini': 'Emilia-Romagna',
    
    # Puglia
    'bari': 'Puglia',
    'brindisi': 'Puglia',
    'foggia': 'Puglia',
    'lecce': 'Puglia',
    'taranto': 'Puglia',
    
    # Toscana
    'firenze': 'Toscana',
    'arezzo': 'Toscana',
    'grosseto': 'Toscana',
    'livorno': 'Toscana',
    'lucca': 'Toscana',
    'massa': 'Toscana',
    'carrara': 'Toscana',
    'pisa': 'Toscana',
    'pistoia': 'Toscana',
    'prato': 'Toscana',
    'siena': 'Toscana',
    'bagno a ripoli': 'Toscana',
    'calenzano': 'Toscana',
    'campi bisenzio': 'Toscana',
    'capannori': 'Toscana',
    'cascina': 'Toscana',
    'castagneto carducci': 'Toscana',
    'castelfiorentino': 'Toscana',
    'castiglione della pescaia': 'Toscana',
    'cecina': 'Toscana',
    'certaldo': 'Toscana',
    'chianciano terme': 'Toscana',
    'colle di val d\'elsa': 'Toscana',
    'cortona': 'Toscana',
    'empoli': 'Toscana',
    'figline valdarno': 'Toscana',
    'follonica': 'Toscana',
    'forte dei marmi': 'Toscana',
    'fucecchio': 'Toscana',
    'montecatini terme': 'Toscana',
    'montepulciano': 'Toscana',
    'pescia': 'Toscana',
    'pietrasanta': 'Toscana',
    'piombino': 'Toscana',
    'poggibonsi': 'Toscana',
    'pontassieve': 'Toscana',
    'pontedera': 'Toscana',
    'portoferraio': 'Toscana',
    'rosignano marittimo': 'Toscana',
    'san gimignano': 'Toscana',
    'san miniato': 'Toscana',
    'scandicci': 'Toscana',
    'sesto fiorentino': 'Toscana',
    'viareggio': 'Toscana',
    'volterra': 'Toscana',
    
    # Calabria
    'catanzaro': 'Calabria',
    'cosenza': 'Calabria',
    'crotone': 'Calabria',
    'reggio calabria': 'Calabria',
    'vibo valentia': 'Calabria',
    
    # Sardegna
    'cagliari': 'Sardegna',
    'carbonia': 'Sardegna',
    'nuoro': 'Sardegna',
    'olbia': 'Sardegna',
    'oristano': 'Sardegna',
    'sassari': 'Sardegna',
    
    # Liguria
    'genova': 'Liguria',
    'imperia': 'Liguria',
    'la spezia': 'Liguria',
    'savona': 'Liguria',
    'finale ligure': 'Liguria',
    'alassio': 'Liguria',
    'albenga': 'Liguria',
    'albisola': 'Liguria',
    'arenzano': 'Liguria',
    'bordighera': 'Liguria',
    'camogli': 'Liguria',
    'chiavari': 'Liguria',
    'lavagna': 'Liguria',
    'loano': 'Liguria',
    'pietra ligure': 'Liguria',
    'rapallo': 'Liguria',
    'recco': 'Liguria',
    'sanremo': 'Liguria',
    'santa margherita ligure': 'Liguria',
    'sestri levante': 'Liguria',
    'taggia': 'Liguria',
    'ventimiglia': 'Liguria',
    'varazze': 'Liguria',
    
    # Marche
    'ancona': 'Marche',
    'ascoli piceno': 'Marche',
    'fermo': 'Marche',
    'macerata': 'Marche',
    'pesaro': 'Marche',
    'urbino': 'Marche',
    
    # Abruzzo
    'l\'aquila': 'Abruzzo',
    'laquila': 'Abruzzo',
    'chieti': 'Abruzzo',
    'pescara': 'Abruzzo',
    'teramo': 'Abruzzo',
    
    # Friuli-Venezia Giulia
    'trieste': 'Friuli-Venezia Giulia',
    'gorizia': 'Friuli-Venezia Giulia',
    'pordenone': 'Friuli-Venezia Giulia',
    'udine': 'Friuli-Venezia Giulia',
    'friuli': 'Friuli-Venezia Giulia',
    'friuli venezia giulia': 'Friuli-Venezia Giulia',
    'venezia giulia': 'Friuli-Venezia Giulia',
    'friuli-venezia giulia': 'Friuli-Venezia Giulia',
    
    # Trentino-Alto Adige
    'trento': 'Trentino',
    'bolzano': 'Trentino',
    
    # Umbria
    'perugia': 'Umbria',
    'terni': 'Umbria',
    
    # Basilicata
    'potenza': 'Basilicata',
    'matera': 'Basilicata',
    
    # Molise
    'campobasso': 'Molise',
    'isernia': 'Molise',
    
    # Valle d'Aosta
    'aosta': 'Valle d Aosta',
}

def detect_location_in_query(query):
    """
    Detect city/location references in a query and map them to regions.
    
    Args:
        query (str): The user's search query text
        
    Returns:
        tuple: (cleaned_query, region_name)
            - cleaned_query: query with location references removed
            - region_name: detected region or None if no location found
    """
    if not query:
        return query, None
    
    # Convert query to lowercase
    query_lower = query.lower()
    
    # Check for direct city matches
    detected_region = None
    detected_city = None
    
    # Check each city in our map
    for city, region in CITY_TO_REGION_MAP.items():
        # Check for exact word match (with word boundaries)
        if f" {city} " in f" {query_lower} " or query_lower.startswith(f"{city} ") or query_lower.endswith(f" {city}") or query_lower == city:
            detected_city = city
            detected_region = region
            break
    
    # If a city was found, remove it from the query
    cleaned_query = query
    if detected_city:
        # Remove the city from the query using regex-like replacements
        cleaned_query = query_lower.replace(detected_city, "").strip()
        
        # Clean up any double spaces
        while "  " in cleaned_query:
            cleaned_query = cleaned_query.replace("  ", " ")
            
        # If the query equals the city name (or becomes empty after removal),
        # we should treat this as a region-only search with no additional text filter
        if not cleaned_query or cleaned_query.isspace():
            # Log this special case
            print(f"Query '{query}' was fully consumed by location '{detected_city}', setting cleaned_query to empty")
            cleaned_query = ""
    
    return cleaned_query, detected_region