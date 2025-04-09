"""
Geocoding and distance calculation utilities for FindMyCure Italia.
This module provides functionality to geocode addresses and calculate 
distances between coordinates.
"""

import requests
import math
import re
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Constants for geocoding
NOMINATIM_API = "https://nominatim.openstreetmap.org/search"
NOMINATIM_REVERSE_API = "https://nominatim.openstreetmap.org/reverse"
DEFAULT_COUNTRY = "Italy"

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
        response = requests.get(NOMINATIM_API, params=params, headers=headers)
        results = response.json()
        
        if results and len(results) > 0:
            # Get the first (best) match
            result = results[0]
            return {
                'lat': float(result['lat']),
                'lon': float(result['lon']),
                'display_name': result['display_name'],
                'address': result.get('address', {})
            }
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
        response = requests.get(NOMINATIM_API, params=params, headers=headers)
        results = response.json()
        
        if results and len(results) > 0:
            result = results[0]
            return {
                'lat': float(result['lat']),
                'lon': float(result['lon'])
            }
    except Exception as e:
        logger.error(f"Error geocoding facility address: {str(e)}")
    
    return None

def find_facilities_near_address(query_text, facilities, max_distance=10.0):
    """
    Find facilities near a specified address.
    
    Args:
        query_text (str): The address query text
        facilities (list): List of medical facilities
        max_distance (float): Maximum distance in kilometers (default: 10km)
        
    Returns:
        list: Facilities sorted by distance with distance info added
    """
    # Parse and geocode the address
    address_components = parse_address(query_text)
    if not address_components:
        return []
    
    geocoded_address = geocode_address(address_components)
    if not geocoded_address:
        return []
    
    # Save the search coordinates
    search_lat = geocoded_address['lat']
    search_lon = geocoded_address['lon']
    search_display = geocoded_address['display_name']
    
    # Calculate distances for each facility
    facilities_with_distance = []
    
    for facility in facilities:
        # Get facility coordinates (ideally these would be stored in the database)
        # For now, we'll geocode the facility address on-the-fly
        facility_coords = extract_coordinates_from_address(facility.address, facility.city)
        
        if facility_coords:
            facility_lat = facility_coords['lat']
            facility_lon = facility_coords['lon']
            
            # Calculate distance
            distance = calculate_distance(search_lat, search_lon, facility_lat, facility_lon)
            
            # Only include facilities within the specified distance
            if distance <= max_distance:
                # Add distance to the facility object (temporary attribute)
                facility.distance = distance
                facility.distance_text = f"{distance:.1f} km"
                facilities_with_distance.append(facility)
    
    # Sort by distance
    facilities_with_distance.sort(key=lambda x: x.distance)
    
    # Add search location info to the results
    return {
        'facilities': facilities_with_distance,
        'search_location': {
            'lat': search_lat,
            'lon': search_lon,
            'display_name': search_display
        }
    }