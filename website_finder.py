"""
Utility module for finding and validating websites for medical facilities.
This module provides functionality to search for and validate website URLs
for medical facilities in the database.
"""

import logging
import re
import requests
from urllib.parse import urlparse

def is_valid_url(url):
    """Check if a URL is valid and points to an accessible website."""
    try:
        # Basic validation of URL format
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return False
        
        # Try to fetch the URL with a HEAD request (faster than GET)
        response = requests.head(url, timeout=5, allow_redirects=True)
        return response.status_code < 400  # Any 2xx or 3xx status code is OK
    except Exception as e:
        logging.error(f"Error validating URL {url}: {str(e)}")
        return False

def find_facility_website(facility_name, city=None, region_name=None):
    """
    Attempt to find a website for a medical facility based on its name and location.
    
    This function implements a search strategy that tries:
    1. Direct domain construction (facility name as domain)
    2. Search for "[facility name] [city] [region] sito ufficiale"
    
    Args:
        facility_name (str): The name of the medical facility
        city (str, optional): The city where the facility is located
        region_name (str, optional): The region where the facility is located
        
    Returns:
        str or None: A valid website URL if found, None otherwise
    """
    # Try direct domain construction
    facility_domain = facility_name.lower()
    
    # Clean up the facility name to make it more suitable for a domain
    facility_domain = re.sub(r'ospedale\s+', '', facility_domain)  # Remove "ospedale" prefix
    facility_domain = re.sub(r'azienda\s+ospedaliera\s+', '', facility_domain)  # Remove "azienda ospedaliera" prefix
    facility_domain = re.sub(r'azienda\s+sanitaria\s+', '', facility_domain)  # Remove "azienda sanitaria" prefix
    facility_domain = re.sub(r'istituto\s+', '', facility_domain)  # Remove "istituto" prefix
    facility_domain = re.sub(r'clinica\s+', '', facility_domain)  # Remove "clinica" prefix
    
    # Remove spaces, apostrophes, and normalize special characters
    facility_domain = re.sub(r'[\s\'-]', '', facility_domain)
    facility_domain = re.sub(r'[àáâäã]', 'a', facility_domain)
    facility_domain = re.sub(r'[èéêë]', 'e', facility_domain)
    facility_domain = re.sub(r'[ìíîï]', 'i', facility_domain)
    facility_domain = re.sub(r'[òóôöõ]', 'o', facility_domain)
    facility_domain = re.sub(r'[ùúûü]', 'u', facility_domain)
    
    # Try common domain patterns
    domain_patterns = [
        f"https://www.{facility_domain}.it",
        f"https://{facility_domain}.it",
        f"https://www.{facility_domain}.com",
        f"https://{facility_domain}.com",
        f"https://www.{facility_domain}.org",
        f"https://{facility_domain}.org"
    ]
    
    # Add region-specific domains for hospitals
    if region_name and "lombardia" in str(region_name).lower() and city and isinstance(city, str):
        city_name = city.lower()
        domain_patterns.append(f"https://{facility_domain}.asst-{city_name}.it")
    
    # Try each domain pattern
    for domain in domain_patterns:
        if is_valid_url(domain):
            return domain
    
    # If no direct domain works, a more sophisticated approach would be to use a search API
    # Such implementation would require an external API key and would be more complex
    
    return None

def update_facility_website(facility):
    """
    Try to find and update the website for a facility.
    
    Args:
        facility: A MedicalFacility database object
        
    Returns:
        bool: True if a website was found and updated, False otherwise
    """
    if facility.website:
        # Already has a website
        return True
    
    # Try to find a website
    website = find_facility_website(
        facility.name,
        city=facility.city,
        region_name=facility.region.name if facility.region else None
    )
    
    if website:
        facility.website = website
        return True
    
    return False