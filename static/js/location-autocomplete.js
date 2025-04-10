/**
 * Location Autocomplete with Nominatim API
 * This script implements location autocomplete using OpenStreetMap's Nominatim API
 */

document.addEventListener('DOMContentLoaded', function() {
    // Elements
    const locationInput = document.getElementById('location-input');
    const locationSuggestions = document.getElementById('location-suggestions');
    const locationForm = document.getElementById('location-form');
    const latitudeInput = document.getElementById('latitude');
    const longitudeInput = document.getElementById('longitude');
    const searchButton = document.getElementById('search-button');
    const loadingSpinner = document.getElementById('loading-spinner');
    
    let selectedLocation = null;
    let typingTimer;
    const doneTypingInterval = 500; // Wait 500ms after user stops typing
    
    // Only initialize if we have the location input on this page
    if (!locationInput) return;
    
    // Function to fetch location suggestions from Nominatim
    function fetchSuggestions(query) {
        if (query.length < 3) {
            locationSuggestions.innerHTML = '';
            locationSuggestions.style.display = 'none';
            return;
        }
        
        // Show loading indicator
        locationInput.classList.add('loading');
        
        // Build API URL with parameters
        const apiUrl = new URL('https://nominatim.openstreetmap.org/search');
        apiUrl.searchParams.append('q', query);
        apiUrl.searchParams.append('format', 'json');
        apiUrl.searchParams.append('countrycodes', 'it'); // Italy only
        apiUrl.searchParams.append('limit', '5'); // Limit to 5 results
        apiUrl.searchParams.append('addressdetails', '1'); // Get address details
        
        fetch(apiUrl.toString(), {
            headers: {
                'User-Agent': 'FindMyCure-Italia/1.0'
            }
        })
        .then(response => response.json())
        .then(data => {
            locationSuggestions.innerHTML = '';
            
            if (data.length === 0) {
                locationSuggestions.style.display = 'none';
                locationInput.classList.remove('loading');
                return;
            }
            
            data.forEach(location => {
                const item = document.createElement('div');
                item.className = 'location-suggestion-item';
                
                // Format location display: use name and important parts of address
                let displayName = location.display_name;
                if (displayName.length > 80) {
                    // Shorten very long names
                    const parts = displayName.split(', ');
                    // Take first part, then skip to region/province level
                    displayName = [
                        parts[0], 
                        parts.length > 2 ? parts[parts.length - 3] : '',
                        parts.length > 1 ? parts[parts.length - 2] : '',
                        'Italia'
                    ].filter(p => p).join(', ');
                }
                
                item.textContent = displayName;
                
                // Store full location data
                item.dataset.lat = location.lat;
                item.dataset.lon = location.lon;
                item.dataset.displayName = displayName;
                
                item.addEventListener('click', function() {
                    // Set the input value to the selected location name
                    locationInput.value = this.dataset.displayName;
                    
                    // Store coordinates in hidden inputs
                    latitudeInput.value = this.dataset.lat;
                    longitudeInput.value = this.dataset.lon;
                    
                    // Store selected location
                    selectedLocation = {
                        lat: this.dataset.lat,
                        lon: this.dataset.lon,
                        displayName: this.dataset.displayName
                    };
                    
                    // Hide suggestions
                    locationSuggestions.style.display = 'none';
                });
                
                locationSuggestions.appendChild(item);
            });
            
            // Show suggestions
            locationSuggestions.style.display = 'block';
            locationInput.classList.remove('loading');
        })
        .catch(error => {
            console.error('Error fetching location suggestions:', error);
            locationInput.classList.remove('loading');
            locationSuggestions.style.display = 'none';
        });
    }
    
    // Handle input changes (with debounce)
    locationInput.addEventListener('input', function() {
        clearTimeout(typingTimer);
        
        // Clear stored location when user types
        selectedLocation = null;
        latitudeInput.value = '';
        longitudeInput.value = '';
        
        const query = this.value.trim();
        
        // Don't search for very short queries
        if (query.length < 3) {
            locationSuggestions.innerHTML = '';
            locationSuggestions.style.display = 'none';
            return;
        }
        
        // Set a timer to wait for user to stop typing
        typingTimer = setTimeout(function() {
            fetchSuggestions(query);
        }, doneTypingInterval);
    });
    
    // Close suggestions when clicking outside
    document.addEventListener('click', function(event) {
        if (!locationInput.contains(event.target) && !locationSuggestions.contains(event.target)) {
            locationSuggestions.style.display = 'none';
        }
    });
    
    // Form submission handling
    if (locationForm) {
        locationForm.addEventListener('submit', function(event) {
            // Show loading spinner
            if (searchButton && loadingSpinner) {
                searchButton.style.display = 'none';
                loadingSpinner.style.display = 'inline-block';
            }
            
            // If user hasn't selected from dropdown but has text, try to geocode it
            if (!selectedLocation && locationInput.value.trim().length > 0) {
                // Allow form to submit normally - backend will handle geocoding
            }
        });
    }
    
    // Prevent suggestions from closing when clicking inside the suggestions div
    locationSuggestions.addEventListener('click', function(event) {
        event.stopPropagation();
    });
    
    // Apply some initial styling
    locationSuggestions.style.display = 'none';
});