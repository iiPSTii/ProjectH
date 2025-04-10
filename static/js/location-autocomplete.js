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
    let lastInputValue = '';  // Track the last known input value for autofill detection
    
    // Only initialize if we have the location input on this page
    if (!locationInput) return;
    
    // Function to check for browser/phone autofill
    function checkForAutofill() {
        const currentValue = locationInput.value.trim();
        
        // If value has changed without an input event, it's likely an autofill
        if (currentValue !== lastInputValue && currentValue.length > 2) {
            console.log('Autofill detected:', currentValue);
            lastInputValue = currentValue;
            
            // Trigger the search for this value
            fetchSuggestions(currentValue);
        }
    }
    
    // Setup autofill detection
    // Check right after page load (for page reload with browser-saved values)
    setTimeout(checkForAutofill, 500);
    
    // Check on focus events (browsers often autofill on focus)
    locationInput.addEventListener('focus', function() {
        setTimeout(checkForAutofill, 100);
    });
    
    // Also check on click, which can trigger autofill on mobile
    locationInput.addEventListener('click', function() {
        setTimeout(checkForAutofill, 100);
    });
    
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
        apiUrl.searchParams.append('viewbox', '6.6272,47.0907,18.7844,35.4897'); // Bounding box for Italy
        apiUrl.searchParams.append('bounded', '1'); // Restrict to bounding box
        
        // Optimize search for Italian addresses
        const isStreetAddress = query.toLowerCase().includes('via') || 
                               query.toLowerCase().includes('piazza') || 
                               query.toLowerCase().includes('corso') ||
                               query.toLowerCase().includes('viale') || 
                               query.toLowerCase().includes('strada') ||
                               query.toLowerCase().includes('largo');

        if (isStreetAddress) {
            // For street addresses, use more specialized parameters
            apiUrl.searchParams.set('street', query);
            apiUrl.searchParams.delete('q'); // Remove the generic query
            
            // Filter by place type to prioritize addresses over administrative regions
            apiUrl.searchParams.append('featuretype', 'building');
            apiUrl.searchParams.append('featuretype', 'highway');
            apiUrl.searchParams.append('featuretype', 'amenity');
        }
        
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
                
                // Enhanced formatting for display names to be more user-friendly
                let displayName = '';
                
                // Check if we have address details to use for better formatting
                if (location.address) {
                    // For street addresses, create a clear format
                    if (location.address.road || location.address.pedestrian) {
                        const street = location.address.road || location.address.pedestrian;
                        const houseNumber = location.address.house_number || '';
                        const city = location.address.city || location.address.town || location.address.village || location.address.municipality || '';
                        const province = location.address.state || location.address.county || '';
                        
                        // Format: "Via Roma 10, Milano, Lombardia, Italia"
                        let formattedParts = [];
                        if (street) {
                            let streetPart = street;
                            if (houseNumber) streetPart += ' ' + houseNumber;
                            formattedParts.push(streetPart);
                        }
                        if (city) formattedParts.push(city);
                        if (province) formattedParts.push(province);
                        formattedParts.push('Italia');
                        
                        displayName = formattedParts.join(', ');
                    }
                    // For cities and towns, create a simple format
                    else if (location.address.city || location.address.town || location.address.village) {
                        const city = location.address.city || location.address.town || location.address.village;
                        const province = location.address.state || location.address.county || '';
                        
                        // Format: "Milano, Lombardia, Italia"
                        displayName = [city, province, 'Italia'].filter(p => p).join(', ');
                    }
                }
                
                // Fallback to default name processing if address details don't yield good results
                if (!displayName) {
                    displayName = location.display_name;
                    // Shorten very long names
                    if (displayName.length > 60) {
                        const parts = displayName.split(', ');
                        // Take first part, then skip to region/province level
                        displayName = [
                            parts[0], 
                            parts.length > 2 ? parts[parts.length - 3] : '',
                            parts.length > 1 ? parts[parts.length - 2] : '',
                            'Italia'
                        ].filter(p => p).join(', ');
                    }
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
                    
                    // Also update the query_text hidden field with the display name
                    // This ensures compatibility with the backend which expects query_text
                    if (document.getElementById('query_text')) {
                        document.getElementById('query_text').value = this.dataset.displayName;
                    }
                    
                    // Store selected location
                    selectedLocation = {
                        lat: this.dataset.lat,
                        lon: this.dataset.lon,
                        displayName: this.dataset.displayName
                    };
                    
                    // Show the location selected indicator
                    const indicator = document.getElementById('location-selected-indicator');
                    if (indicator) {
                        indicator.classList.remove('d-none');
                        
                        // Update the location name in the indicator
                        const locationNameIndicator = indicator.querySelector('.location-name');
                        if (locationNameIndicator) {
                            locationNameIndicator.textContent = this.dataset.displayName.substring(0, 25) + 
                                                (this.dataset.displayName.length > 25 ? '...' : '');
                        }
                        
                        // Update the radius indicator
                        const radiusElement = document.getElementById('radius');
                        const radiusValue = radiusElement ? radiusElement.value : '30';
                        const radiusIndicator = indicator.querySelector('.radius-indicator');
                        if (radiusIndicator) {
                            radiusIndicator.textContent = radiusValue + ' km';
                        }
                    }
                    
                    // Keep the search button clean with just "Cerca Strutture"
                    const searchButton = document.getElementById('searchButton');
                    if (searchButton) {
                        // Focus on the search button for better UX
                        searchButton.focus();
                        
                        // Scroll to the search button for better visibility
                        searchButton.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    }
                    
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
        
        // Update last known value to detect autofill
        lastInputValue = query;
        
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
    
    // Special case for browser autofill - add more events that might trigger it
    locationInput.addEventListener('change', function() {
        checkForAutofill();
    });
    
    // Listen for autocomplete events
    locationInput.addEventListener('autocomplete', function() {
        checkForAutofill();
    });
    
    // Listen for autofill events - newer spec
    locationInput.addEventListener('autocompleteerror', function() {
        checkForAutofill();
    });
    
    // For mobile browsers that might use a different event
    document.addEventListener('pageshow', function() {
        setTimeout(checkForAutofill, 500);
    });
    
    // Close suggestions when clicking outside
    document.addEventListener('click', function(event) {
        if (!locationInput.contains(event.target) && !locationSuggestions.contains(event.target)) {
            locationSuggestions.style.display = 'none';
        }
    });
    
    // Form submission handling - Now has different behavior because search-button is no longer submit
    const searchForm = document.getElementById('searchForm');
    if (searchForm) {
        searchForm.addEventListener('submit', function(event) {
            // Make sure query_text has latest location input if user typed something but didn't click a suggestion
            if (!selectedLocation && locationInput && locationInput.value.trim().length > 0) {
                const queryTextInput = document.getElementById('query_text');
                if (queryTextInput) {
                    queryTextInput.value = locationInput.value.trim();
                }
            }
            
            // Make sure query_text has the location name if coordinates are present
            if (latitudeInput.value && longitudeInput.value) {
                const queryTextInput = document.getElementById('query_text');
                if (queryTextInput && !queryTextInput.value) {
                    queryTextInput.value = locationInput.value.trim();
                }
            }
            
            // Main search button loading spinner is handled in main.js
        });
    }
    
    // Search button click (now type="button")
    if (searchButton) {
        searchButton.addEventListener('click', function() {
            // Show loading spinner
            if (loadingSpinner) {
                searchButton.innerHTML = '<i class="fas fa-map-marked-alt me-2"></i> Applica';
                loadingSpinner.style.display = 'inline-block';
            }
            
            // If user hasn't selected from dropdown but has text, update query_text for geocoding
            if (!selectedLocation && locationInput.value.trim().length > 0) {
                const queryTextInput = document.getElementById('query_text');
                if (queryTextInput) {
                    queryTextInput.value = locationInput.value.trim();
                }
            }
            
            // The click handler calls applyLocationSearch() instead of submitting the form
            setTimeout(() => {
                if (loadingSpinner) {
                    loadingSpinner.style.display = 'none';
                }
            }, 500);
        });
    }
    
    // Prevent suggestions from closing when clicking inside the suggestions div
    locationSuggestions.addEventListener('click', function(event) {
        event.stopPropagation();
    });
    
    // Apply some initial styling
    locationSuggestions.style.display = 'none';
});