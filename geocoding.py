"""
Geocoding and distance calculation utilities for FindMyCure Italia.
This module provides functionality to geocode addresses and calculate 
distances between coordinates.
"""

import requests
import math
import re
import logging
import time

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Constants for geocoding
NOMINATIM_API = "https://nominatim.openstreetmap.org/search"
NOMINATIM_REVERSE_API = "https://nominatim.openstreetmap.org/reverse"
DEFAULT_COUNTRY = "Italy"

# Rate limiting and timeouts
GEOCODING_TIMEOUT = 3  # seconds
GEOCODING_DELAY = 1.0  # seconds between requests
MAX_FACILITIES_TO_GEOCODE = 30  # maximum number of facilities to geocode

# Functions for address parsing and geocoding
def parse_address(query_text):
    """
    Parse a query text to extract address components.
    
    Args:
        query_text (str): The user's search query containing address information
        
    Returns:
        dict: Extracted address components (street, number, city)
    """
    # Define common Italian cities - expanded list
    common_cities = [
        'milano', 'roma', 'napoli', 'torino', 'bologna', 'firenze', 'genova', 
        'palermo', 'bari', 'venezia', 'verona', 'padova', 'catania', 'messina', 
        'salerno', 'parma', 'modena', 'reggio', 'pisa', 'livorno', 'siena'
    ]
    
    # Check if we have explicit city information (after a comma or at the end)
    city = None
    street_text = query_text
    
    # Check comma-separated parts first - they take priority
    if ',' in query_text:
        query_parts = query_text.split(',')
        
        # If we have comma-separated parts, the last part might be the city
        if len(query_parts) > 1:
            city_part = query_parts[-1].strip().lower()
            
            # Check if the last part contains a known city name
            for common_city in common_cities:
                if common_city in city_part:
                    city = common_city.title()
                    break
            
            # If no known city found, but we have a non-empty part after comma, use it as city
            if not city and len(city_part) > 2:
                city = city_part.title()
            
            # Use only the part before the comma for street parsing
            street_text = ','.join(query_parts[:-1])
    
    # Try to match street pattern first with the street part
    # Basic address pattern: "via/corso/piazza name number [potential city]"
    street_prefixes = ['via', 'corso', 'piazza', 'viale', 'vicolo', 'strada', 'largo']
    prefix_pattern = '|'.join(street_prefixes)
    address_pattern = r'(' + prefix_pattern + r')\s+([a-zA-Z\s\']+)\s+(\d+)?(?:\s+([a-zA-Z\s]+))?'
    
    match = re.search(address_pattern, street_text.lower())
    
    if match:
        street_type = match.group(1)
        street_name = match.group(2).strip()
        street_number = match.group(3) or ""
        
        # If we have city from comma-separated part, use it
        # Otherwise, try to extract from the match
        if not city and match.group(4):
            potential_city = match.group(4).strip()
            
            # Check if this might be a city name
            for common_city in common_cities:
                if common_city in potential_city.lower():
                    city = common_city.title()
                    break
            
            # If not found in our common cities list but looks like a city name, use it
            if not city and len(potential_city) > 3:
                # Check if this potential city appears somewhere in the original query
                # in case it was repeated or better specified elsewhere
                for word in query_text.lower().split():
                    if word in common_cities:
                        city = word.title()
                        break
                
                # If still no match, use what we extracted
                if not city:
                    city = potential_city.title()
        
        # If still no city, check the full query for any city mentions
        if not city:
            for common_city in common_cities:
                if common_city in query_text.lower():
                    city = common_city.title()
                    break
        
        return {
            "street": f"{street_type} {street_name}",
            "number": street_number,
            "city": city or ""
        }
    
    # If we couldn't match the street pattern but have a city, try a simple approach
    # Extract any street prefix and create a partial address
    if not match:
        for prefix in street_prefixes:
            if f" {prefix} " in f" {street_text.lower()} " or street_text.lower().startswith(f"{prefix} "):
                # Extract from prefix to the next word boundary
                pattern = r'\b' + prefix + r'\s+([^\s,]+)'
                simple_match = re.search(pattern, street_text.lower())
                if simple_match:
                    return {
                        "street": f"{prefix} {simple_match.group(1)}",
                        "number": "",
                        "city": city or ""
                    }
    
    return None

def geocode_address(address_components):
    """
    Geocode an address using OpenStreetMap Nominatim API.
    
    Args:
        address_components (dict): Address components to geocode
        
    Returns:
        dict: Geocoding results containing lat, lon coordinates
    """
    if not address_components:
        return None
    
    # Build the search query string
    street = f"{address_components['street']} {address_components['number']}".strip()
    search_query = street
    
    if address_components['city']:
        search_query += f", {address_components['city']}"
    
    search_query += f", {DEFAULT_COUNTRY}"
    
    # Request parameters
    params = {
        'q': search_query,
        'format': 'json',
        'limit': 1,
        'countrycodes': 'it'
    }
    
    headers = {
        'User-Agent': 'FindMyCure-Italia/1.0'
    }
    
    try:
        # Add timeout to prevent hanging
        response = requests.get(NOMINATIM_API, params=params, headers=headers, timeout=GEOCODING_TIMEOUT)
        results = response.json()
        
        if results and len(results) > 0:
            # Get the first (best) match
            result = results[0]
            # Add a small delay to respect rate limits
            time.sleep(GEOCODING_DELAY)
            # Extract address components
            address_parts = result.get('address', {})
            # Get region from the result (called 'state' in Nominatim's response)
            region = address_parts.get('state', '')
            
            return {
                'lat': float(result['lat']),
                'lon': float(result['lon']),
                'display_name': result['display_name'],
                'address': address_parts,
                'region': region
            }
    except requests.exceptions.Timeout:
        logger.error(f"Timeout geocoding address: {search_query}")
    except Exception as e:
        logger.error(f"Error geocoding address: {str(e)}")
    
    return None

def calculate_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the distance between two points using the Haversine formula.
    
    Args:
        lat1, lon1: Coordinates of the first point
        lat2, lon2: Coordinates of the second point
        
    Returns:
        float: Distance in kilometers
    """
    # Radius of the Earth in kilometers
    R = 6371.0
    
    # Convert lat and lon from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    # Calculate differences
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    
    # Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    
    return distance

def is_address_query(query_text):
    """
    Determine if a search query appears to be an address.
    
    Args:
        query_text (str): The search query text
        
    Returns:
        bool: True if the query seems to be an address
    """
    if not query_text:
        return False
    
    # Check for common street type prefixes
    street_prefixes = ['via', 'corso', 'piazza', 'viale', 'vicolo', 'strada', 'largo']
    
    # Normalize query text
    query_lower = query_text.lower()
    
    # Check if any of the prefixes appear at the beginning of a word
    for prefix in street_prefixes:
        if re.search(r'\b' + prefix + r'\b', query_lower):
            return True
    
    return False

def extract_address_part(query_text):
    """
    Extract the address part from a complex query containing other terms.
    
    Args:
        query_text (str): The search query text
        
    Returns:
        str: The extracted address part, or original query if no address is found
    """
    # Extract address using pattern matching
    street_prefixes = ['via', 'corso', 'piazza', 'viale', 'vicolo', 'strada', 'largo']
    
    # Normalize query text
    query_lower = query_text.lower()
    
    # Find the first occurrence of a street prefix
    start_index = -1
    prefix_found = None
    
    for prefix in street_prefixes:
        pattern = r'\b' + prefix + r'\b'
        match = re.search(pattern, query_lower)
        if match and (start_index == -1 or match.start() < start_index):
            start_index = match.start()
            prefix_found = prefix
    
    if start_index >= 0:
        # Extract everything from the prefix to the end
        address_part = query_text[start_index:]
        
        # Define common Italian cities - expanded list
        common_cities = [
            'milano', 'roma', 'napoli', 'torino', 'bologna', 'firenze', 'genova', 
            'palermo', 'bari', 'venezia', 'verona', 'padova', 'catania', 'messina', 
            'salerno', 'parma', 'modena', 'reggio', 'pisa', 'livorno', 'siena'
        ]
        
        # Check if address_part already has a city in it (comma-separated or just at the end)
        already_has_city = False
        extracted_city = None
        
        # First check for comma-separated city
        if ',' in address_part:
            parts = address_part.split(',')
            for city in common_cities:
                if any(city in part.lower().strip() for part in parts):
                    already_has_city = True
                    break
        
        # If address doesn't already have a city with comma, check if it has a city at the end
        if not already_has_city:
            # Try to extract city at the end of the address (after street number)
            # Pattern to match a street address: "street_type street_name number [potential city]"
            street_pattern = r'(' + '|'.join(street_prefixes) + r')\s+([a-zA-Z\s\']+)\s+(\d+)(?:\s+([a-zA-Z\s]+))?'
            street_match = re.search(street_pattern, address_part.lower())
            
            if street_match and street_match.group(4):  # If we have text after the street number
                potential_city = street_match.group(4).strip()
                
                # Check if this looks like a city name
                for city in common_cities:
                    if city in potential_city:
                        extracted_city = city
                        already_has_city = True
                        break
        
        # Next, check if we need to add a city from the original query
        if not already_has_city:
            # Check the original query for city mentions
            city_in_query = None
            for city in common_cities:
                if city in query_lower and city not in address_part.lower():
                    city_in_query = city
                    break
                    
            # If we found a city in the query and it's not already in the address, add it
            if city_in_query:
                address_part += f", {city_in_query.title()}"
        
        # If we extracted a city from the address but it's not properly formatted with a comma,
        # reformat the address to have the city comma-separated
        elif extracted_city and ',' not in address_part:
            # Replace the city mention with a comma-separated version
            address_without_city = re.sub(r'\s+' + extracted_city + r'\b', '', address_part, flags=re.IGNORECASE)
            address_part = f"{address_without_city}, {extracted_city.title()}"
        
        return address_part
    
    return query_text

def extract_coordinates_from_facilities(facilities):
    """
    Extract coordinates from a list of medical facilities.
    
    Args:
        facilities (list): List of MedicalFacility objects
        
    Returns:
        list: List of facilities with added coordinates and defaults for missing values
    """
    facilities_with_coords = []
    
    for facility in facilities:
        # Try to parse coordinates from address
        coords = extract_coordinates_from_address(facility.address, facility.city)
        if coords:
            facility.lat = coords['lat']
            facility.lon = coords['lon']
            facilities_with_coords.append(facility)
    
    return facilities_with_coords

def extract_coordinates_from_address(address, city=None):
    """
    Extract coordinates from an address string using geocoding.
    
    Args:
        address (str): Address string
        city (str): City name (optional)
        
    Returns:
        dict: Dictionary with lat, lon coordinates or None if geocoding fails
    """
    if not address:
        return None
    
    # Build the search query
    search_query = address
    if city:
        search_query += f", {city}"
    search_query += f", {DEFAULT_COUNTRY}"
    
    # Request parameters
    params = {
        'q': search_query,
        'format': 'json',
        'limit': 1,
        'countrycodes': 'it'
    }
    
    headers = {
        'User-Agent': 'FindMyCure-Italia/1.0'
    }
    
    try:
        # Add timeout to prevent blocking the app
        response = requests.get(NOMINATIM_API, params=params, headers=headers, timeout=GEOCODING_TIMEOUT)
        results = response.json()
        
        if results and len(results) > 0:
            result = results[0]
            # Add a short delay to respect rate limiting
            time.sleep(GEOCODING_DELAY)
            return {
                'lat': float(result['lat']),
                'lon': float(result['lon'])
            }
    except requests.exceptions.Timeout:
        logger.error(f"Timeout while geocoding address: {search_query}")
    except Exception as e:
        logger.error(f"Error geocoding facility address: {str(e)}")
    
    return None

def find_facilities_near_address(query_text, facilities, max_distance=30.0, max_results=50):
    """
    Find facilities near a specified address using stored coordinates.
    
    Args:
        query_text (str): The address query text
        facilities (list): List of medical facilities
        max_distance (float): Maximum distance in kilometers (default: 30km)
        max_results (int): Maximum number of results to return (default: 50)
        
    Returns:
        dict: Dictionary with facilities sorted by distance and search location details
    """
    # Use city-to-region mapping from location_mapping.py for comprehensive city coverage
    from location_mapping import CITY_TO_REGION_MAP
    
    # Special handling for city searches (including multi-word cities like "Finale Ligure")
    query_lower = query_text.strip().lower()
    
    # Check both direct matches and multi-word cities
    is_known_city = query_lower in CITY_TO_REGION_MAP
    
    # If not a direct match, try a more thorough check for multi-word cities
    if not is_known_city:
        # Check for partial matches in the city map (both exact and within multi-word keys)
        for city_key in CITY_TO_REGION_MAP.keys():
            # Check if city_key contains the query or query contains city_key
            if (query_lower in city_key or city_key in query_lower):
                is_known_city = True
                # Override query with the known city name from our mapping
                query_lower = city_key
                logger.info(f"Matched multi-word city: '{query_text}' -> '{city_key}'")
                break
    
    # For any city in our mapping (single or multi-word)
    if is_known_city:
        logger.info(f"Direct city search for: {query_text}")
        # Build a query directly for this city
        search_query = f"{query_text.strip()}, {DEFAULT_COUNTRY}"
        
        # Request parameters
        params = {
            'q': search_query,
            'format': 'json',
            'limit': 1,
            'countrycodes': 'it'
        }
        
        headers = {
            'User-Agent': 'FindMyCure-Italia/1.0'
        }
        
        # Make the request
        try:
            response = requests.get(
                NOMINATIM_API,
                params=params,
                headers=headers,
                timeout=GEOCODING_TIMEOUT
            )
            response.raise_for_status()
            
            # Parse the results
            results = response.json()
            if results:
                # Get the first result
                result = results[0]
                # Extract address components from the result
                address_parts = result.get('address', {})
                
                # Get region from the result (called 'state' in Nominatim's response)
                region = address_parts.get('state', '')
                
                geocoded_address = {
                    'lat': float(result['lat']),
                    'lon': float(result['lon']),
                    'display_name': result['display_name'],
                    'address': address_parts,
                    'region': region
                }
                logger.info(f"Successfully geocoded city: {query_text} -> {geocoded_address['display_name']}")
            else:
                logger.warning(f"No results found for city: {query_text}")
                return {}
        except Exception as e:
            logger.error(f"Error geocoding city: {str(e)}")
            return {}
    else:
        # Normal address processing for non-city-only searches
        # If the query might contain medical terms along with an address,
        # extract just the address part
        address_text = extract_address_part(query_text)
        
        # Parse and geocode the address
        address_components = parse_address(address_text)
        if not address_components:
            logger.warning(f"Could not parse address components from: {address_text}")
            return {}
        
        geocoded_address = geocode_address(address_components)
        if not geocoded_address:
            logger.warning(f"Could not geocode address: {address_text}")
            return {}
    
    # Save the search coordinates
    search_lat = geocoded_address['lat']
    search_lon = geocoded_address['lon']
    search_display = geocoded_address['display_name']
    
    if 'address_text' not in locals():
        address_text = query_text  # In case of city-only search
    
    logger.info(f"Successfully geocoded address: {address_text} -> {search_display}")
    
    # Check for city search to match facilities exactly by city
    city_name = query_lower  # Use the normalized version from earlier
    is_city_search = is_known_city  # Use the result of our comprehensive city check from earlier
    
    # Calculate distances for each facility that has coordinates
    facilities_with_distance = []
    facilities_without_coords = []
    
    # Count geocoded facilities
    geocoded_count = 0
    
    for facility in facilities:
        try:
            # Special case for exact city matches (Parma, Avellino, etc.)
            # Handle variations in city name case and formatting
            facility_city = (facility.city or '').lower()
            if is_city_search and (city_name == facility_city or 
                                 city_name in facility_city.split() or 
                                 facility_city.startswith(city_name)):
                # Add facilities that exactly match the city name, regardless of distance
                # These are guaranteed to be relevant
                facility.distance = 0.1  # Set a very close distance
                facility.distance_text = f"0.1 km"
                facilities_with_distance.append(facility)
                logger.debug(f"Added {city_name} facility {facility.name} with special handling")
                continue
                
            # Use the stored coordinates from the database
            if facility.latitude is not None and facility.longitude is not None:
                geocoded_count += 1
                facility_lat = facility.latitude
                facility_lon = facility.longitude
                
                # Calculate distance
                distance = calculate_distance(search_lat, search_lon, facility_lat, facility_lon)
                
                # Only include facilities within the specified distance
                if distance <= max_distance:
                    # Add distance to the facility object (temporary attribute)
                    # Round to one decimal place for consistency
                    facility.distance = round(distance, 1)
                    facility.distance_text = f"{facility.distance:.1f} km"
                    facilities_with_distance.append(facility)
                    
                    logger.debug(f"Added facility {facility.name} at distance {facility.distance:.1f} km")
            else:
                # Keep track of facilities without coordinates
                facilities_without_coords.append(facility)
        except Exception as e:
            logger.error(f"Error processing facility {facility.name}: {str(e)}")
            continue
    
    # Sort by distance
    facilities_with_distance.sort(key=lambda x: x.distance)
    
    # If this is a city search and we didn't find any facilities, try to find nearby facilities by distance
    if is_city_search and len(facilities_with_distance) == 0:
        # Get the region for this city
        region_name = CITY_TO_REGION_MAP.get(city_name)
        if region_name:
            logger.info(f"No facilities found in {city_name}, looking in {region_name} region using coordinates")
            
            # Try to use the geocoded coordinates to find facilities in the region
            if search_lat and search_lon:
                # Find facilities with coordinates in the same region and calculate actual distances
                found_facilities_by_coords = False
                
                for facility in facilities:
                    # Check if facility is in the same region and has coordinates
                    if (hasattr(facility, 'region') and facility.region and facility.region.name == region_name
                        and facility.latitude is not None and facility.longitude is not None):
                        
                        # Calculate actual distance from search point to facility
                        distance = calculate_distance(search_lat, search_lon, facility.latitude, facility.longitude)
                        
                        # Add facility with actual distance calculation (up to 300km)
                        if distance <= 300.0:
                            facility.distance = round(distance, 1)
                            facility.distance_text = f"{facility.distance:.1f} km"
                            facilities_with_distance.append(facility)
                            found_facilities_by_coords = True
                            logger.debug(f"Added region facility {facility.name} with calculated distance {facility.distance:.1f} km")
                
                # If we still didn't find any facilities with coordinates, fall back to the region-level approach
                if not found_facilities_by_coords:
                    logger.info(f"No facilities with coordinates found in {region_name}, using fallback method")
                    
                    # Find facilities in the same region without checking coordinates
                    for facility in facilities:
                        if hasattr(facility, 'region') and facility.region and facility.region.name == region_name:
                            facility.distance = 25.0  # Set a generic regional distance
                            facility.distance_text = f"~25 km"
                            facilities_with_distance.append(facility)
                            logger.debug(f"Added region-level facility {facility.name} from {region_name} (fallback)")
            else:
                # If we don't have coordinates for the search, fall back to region-level approach
                logger.info(f"No coordinates for {city_name}, falling back to region-level approach")
                
                # Find facilities in the same region
                for facility in facilities:
                    # Check if facility is in the same region (if it has a region)
                    if hasattr(facility, 'region') and facility.region and facility.region.name == region_name:
                        facility.distance = 25.0  # Set a reasonable distance
                        facility.distance_text = f"~25 km"
                        facilities_with_distance.append(facility)
                        logger.debug(f"Added region-level facility {facility.name} from {region_name} (no coords)")
            
            # Limit to 5 facilities from the region
            if len(facilities_with_distance) > 5:
                facilities_with_distance = facilities_with_distance[:5]
                
            if facilities_with_distance:
                logger.info(f"Added {len(facilities_with_distance)} facilities from {region_name} region")
    
    # If we have very few geocoded facilities (less than 5%), try geocoding on-the-fly for a small subset
    if geocoded_count < len(facilities) * 0.05 and len(facilities_with_distance) < 3:
        logger.warning(f"Few geocoded facilities ({geocoded_count}/{len(facilities)}). Trying on-the-fly geocoding for some.")
        
        # Try to geocode a few facilities on-the-fly
        city_name = geocoded_address.get('address', {}).get('city', '')
        if not city_name:
            # Try to extract city from display name
            display_parts = geocoded_address.get('display_name', '').split(',')
            if len(display_parts) > 1:
                city_name = display_parts[1].strip()
        
        # Filter facilities by city if we have a city name
        if city_name:
            city_facilities = [f for f in facilities_without_coords if city_name.lower() in (f.city or '').lower()]
            # Limit to avoid timeouts
            city_facilities = city_facilities[:5]
            
            for facility in city_facilities:
                try:
                    # Geocode on-the-fly
                    coords = extract_coordinates_from_address(facility.address, facility.city)
                    if coords:
                        facility_lat = coords['lat']
                        facility_lon = coords['lon']
                        
                        # Calculate distance
                        distance = calculate_distance(search_lat, search_lon, facility_lat, facility_lon)
                        
                        if distance <= max_distance:
                            facility.distance = round(distance, 1)
                            facility.distance_text = f"{facility.distance:.1f} km"
                            facilities_with_distance.append(facility)
                            
                            # Store coordinates for future lookup (these are temporary and won't be saved to DB)
                            # To permanently save the coordinates, we'd need a separate update function
                            # to avoid circular imports with app.py
                            facility._temp_lat = facility_lat
                            facility._temp_lon = facility_lon
                            facility._temp_geocoded = True
                            
                            logger.info(f"On-the-fly geocoding: Added facility {facility.name} at distance {facility.distance:.1f} km")
                except Exception as e:
                    logger.error(f"Error on-the-fly geocoding for {facility.name}: {str(e)}")
                    continue
    
    # Limit the number of results
    if max_results and len(facilities_with_distance) > max_results:
        facilities_with_distance = facilities_with_distance[:max_results]
    
    # Sort again to include any newly geocoded facilities
    facilities_with_distance.sort(key=lambda x: x.distance)
    
    logger.info(f"Found {len(facilities_with_distance)} facilities within {max_distance} km of {search_display}")
    
    # Add search location info to the results
    return {
        'facilities': facilities_with_distance,
        'search_location': {
            'lat': search_lat,
            'lon': search_lon,
            'display_name': search_display
        }
    }