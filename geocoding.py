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
    # Basic address pattern: "via/corso/piazza name number city"
    address_pattern = r'(via|corso|piazza|viale|vicolo|strada|largo)\s+([a-zA-Z\s\']+)\s+(\d+)?\s*([a-zA-Z\s]+)?'
    match = re.search(address_pattern, query_text.lower())
    
    if match:
        street_type = match.group(1)
        street_name = match.group(2).strip()
        street_number = match.group(3) or ""
        city = match.group(4).strip() if match.group(4) else ""
        
        return {
            "street": f"{street_type} {street_name}",
            "number": street_number,
            "city": city
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
            return {
                'lat': float(result['lat']),
                'lon': float(result['lon']),
                'display_name': result['display_name'],
                'address': result.get('address', {})
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

def find_facilities_near_address(query_text, facilities, max_distance=10.0, max_results=20):
    """
    Find facilities near a specified address using stored coordinates.
    
    Args:
        query_text (str): The address query text
        facilities (list): List of medical facilities
        max_distance (float): Maximum distance in kilometers (default: 10km)
        max_results (int): Maximum number of results to return (default: 20)
        
    Returns:
        dict: Dictionary with facilities sorted by distance and search location details
    """
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
    
    logger.info(f"Successfully geocoded address: {address_text} -> {search_display}")
    
    # Calculate distances for each facility that has coordinates
    facilities_with_distance = []
    facilities_without_coords = []
    
    # Count geocoded facilities
    geocoded_count = 0
    
    for facility in facilities:
        try:
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
                    facility.distance = distance
                    facility.distance_text = f"{distance:.1f} km"
                    facilities_with_distance.append(facility)
                    
                    logger.debug(f"Added facility {facility.name} at distance {distance:.1f} km")
            else:
                # Keep track of facilities without coordinates
                facilities_without_coords.append(facility)
        except Exception as e:
            logger.error(f"Error processing facility {facility.name}: {str(e)}")
            continue
    
    # Sort by distance
    facilities_with_distance.sort(key=lambda x: x.distance)
    
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
                            facility.distance = distance
                            facility.distance_text = f"{distance:.1f} km"
                            facilities_with_distance.append(facility)
                            
                            # Store coordinates for future lookup (these are temporary and won't be saved to DB)
                            # To permanently save the coordinates, we'd need a separate update function
                            # to avoid circular imports with app.py
                            facility._temp_lat = facility_lat
                            facility._temp_lon = facility_lon
                            facility._temp_geocoded = True
                            
                            logger.info(f"On-the-fly geocoding: Added facility {facility.name} at distance {distance:.1f} km")
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