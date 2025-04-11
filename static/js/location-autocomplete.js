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
    let autofillPollingId = null; // ID for the polling interval
    
    // Only initialize if we have the location input on this page
    if (!locationInput) return;
    
    // Start an aggressive polling to detect browser autofill
    // This is necessary because some browsers don't trigger events when autofilling
    const startAutofillPolling = () => {
        if (autofillPollingId) {
            clearInterval(autofillPollingId);
        }
        
        console.log("Starting autofill polling");
        autofillPollingId = setInterval(() => {
            const currentValue = locationInput.value.trim();
            if (currentValue !== lastInputValue && currentValue.length > 2) {
                console.log('Autofill detected through polling:', currentValue);
                lastInputValue = currentValue;
                fetchSuggestions(currentValue);
            }
        }, 100); // Poll every 100ms for changes
        
        // Stop polling after 5 seconds to save resources
        setTimeout(() => {
            if (autofillPollingId) {
                console.log("Stopping autofill polling");
                clearInterval(autofillPollingId);
                autofillPollingId = null;
            }
        }, 5000);
    };
    
    // Function to check for browser/phone autofill
    function checkForAutofill() {
        const currentValue = locationInput.value.trim();
        
        // If value has changed without an input event, it's likely an autofill
        if (currentValue !== lastInputValue && currentValue.length > 2) {
            console.log('Autofill detected:', currentValue);
            lastInputValue = currentValue;
            
            // Usa una soluzione ibrida per l'autofill per supportare entrambi gli scenari
            console.log("Gestisco autofill con soluzione ibrida");
            
            // Usa approccio misto: geocoding per indirizzo completo, e fallback locale
            const geocodeUrl = new URL('https://nominatim.openstreetmap.org/search');
            geocodeUrl.searchParams.append('q', currentValue);
            geocodeUrl.searchParams.append('format', 'json');
            geocodeUrl.searchParams.append('limit', '1');
            geocodeUrl.searchParams.append('countrycodes', 'it');
            
            fetch(geocodeUrl.toString(), {
                headers: {
                    'User-Agent': 'FindMyCure-Italia/1.0'
                }
            })
            .then(response => response.json())
            .then(data => {
                if (data && data.length > 0) {
                    // We got coordinates from API, update hidden fields
                    console.log("Trovate coordinate API:", data[0].lat, data[0].lon);
                    latitudeInput.value = data[0].lat;
                    longitudeInput.value = data[0].lon;
                    
                    // Update query_text
                    if (document.getElementById('query_text')) {
                        document.getElementById('query_text').value = currentValue;
                    }
                    
                    // Attiva l'indicatore verde di conferma
                    const confirmationIndicator = document.getElementById('location-confirmation');
                    if (confirmationIndicator) {
                        confirmationIndicator.classList.add('active');
                    }
                    
                    // Nascondiamo l'indicatore originale
                    const indicator = document.getElementById('location-selected-indicator');
                    if (indicator) {
                        // indicator.classList.remove('d-none'); // Non mostriamo più questo indicatore
                        
                        const locationNameIndicator = indicator.querySelector('.location-name');
                        if (locationNameIndicator) {
                            locationNameIndicator.textContent = currentValue.substring(0, 25) + 
                                (currentValue.length > 25 ? '...' : '');
                        }
                        
                        const radiusElement = document.getElementById('radius');
                        const radiusValue = radiusElement ? radiusElement.value : '30';
                        const radiusIndicator = indicator.querySelector('.radius-indicator');
                        if (radiusIndicator) {
                            radiusIndicator.textContent = radiusValue + ' km';
                        }
                    }
                } else {
                    // API failed, use fallback
                    console.log("Nessun risultato API, uso fallback locale");
                    
                    // Crea location fallback con coordinate di default per Milano
                    locationSuggestions.innerHTML = '';
                    
                    // Create fallback items with coordinates from geocoding
                    // Try to extract regional or city information from the address
                    const addressParts = currentValue.split(',');
                    let regionName = '';
                    
                    // Try to extract region from the address (typically the second-to-last part)
                    if (addressParts.length >= 2) {
                        regionName = addressParts[addressParts.length - 2].trim();
                    }
                    
                    // Map of regions to their approximate center coordinates
                    const regionCoordinates = {
                        'Sardegna': {lat: "40.0690", lon: "9.0212"},
                        'Sardinia': {lat: "40.0690", lon: "9.0212"},
                        'Sicilia': {lat: "37.5990", lon: "14.0154"},
                        'Sicily': {lat: "37.5990", lon: "14.0154"},
                        'Lombardia': {lat: "45.4785", lon: "9.2195"},
                        'Lombardy': {lat: "45.4785", lon: "9.2195"},
                        'Lazio': {lat: "41.9028", lon: "12.4964"},
                        'Toscana': {lat: "43.7711", lon: "11.2486"},
                        'Tuscany': {lat: "43.7711", lon: "11.2486"},
                        'Campania': {lat: "40.8358", lon: "14.2487"},
                        'Piemonte': {lat: "45.0703", lon: "7.6869"},
                        'Piedmont': {lat: "45.0703", lon: "7.6869"},
                        'Puglia': {lat: "41.1187", lon: "16.8718"},
                        'Apulia': {lat: "41.1187", lon: "16.8718"},
                        'Emilia-Romagna': {lat: "44.4938", lon: "11.3426"},
                        'Calabria': {lat: "39.3087", lon: "16.3158"},
                        'Liguria': {lat: "44.4222", lon: "8.9052"},
                        'Veneto': {lat: "45.4408", lon: "12.3155"},
                        'Italia': {lat: "41.8719", lon: "12.5674"},
                        'Italy': {lat: "41.8719", lon: "12.5674"}
                    };
                    
                    // Find coordinates based on the region in the address
                    let coords = {lat: "41.8719", lon: "12.5674"}; // Default to Italy center
                    
                    // Check if we have a region match
                    for (const [region, coordinates] of Object.entries(regionCoordinates)) {
                        if (currentValue.includes(region)) {
                            coords = coordinates;
                            console.log(`Found region match: ${region} -> using coordinates:`, coords);
                            break;
                        }
                    }
                    
                    const fallbackAddresses = [
                        {
                            display_name: currentValue,
                            lat: coords.lat,  // Use region-appropriate coordinates
                            lon: coords.lon
                        }
                    ];
                    
                    fallbackAddresses.forEach(location => {
                        // Update hidden inputs with fallback coordinates
                        latitudeInput.value = location.lat;
                        longitudeInput.value = location.lon;
                        
                        // Also update the query_text hidden field
                        if (document.getElementById('query_text')) {
                            document.getElementById('query_text').value = location.display_name;
                        }
                        
                        // Store selected location
                        selectedLocation = {
                            lat: location.lat,
                            lon: location.lon,
                            displayName: location.display_name
                        };
                        
                        // Attiva l'indicatore verde di conferma
                        const confirmationIndicator = document.getElementById('location-confirmation');
                        if (confirmationIndicator) {
                            confirmationIndicator.classList.add('active');
                        }
                        
                        // Nascondiamo l'indicatore originale
                        const indicator = document.getElementById('location-selected-indicator');
                        if (indicator) {
                            // indicator.classList.remove('d-none'); // Non mostriamo più questo indicatore
                            
                            const locationNameIndicator = indicator.querySelector('.location-name');
                            if (locationNameIndicator) {
                                locationNameIndicator.textContent = location.display_name.substring(0, 25) + 
                                    (location.display_name.length > 25 ? '...' : '');
                            }
                            
                            const radiusElement = document.getElementById('radius');
                            const radiusValue = radiusElement ? radiusElement.value : '30';
                            const radiusIndicator = indicator.querySelector('.radius-indicator');
                            if (radiusIndicator) {
                                radiusIndicator.textContent = radiusValue + ' km';
                            }
                        }
                    });
                }
            })
            .catch(error => {
                console.error("Error geocoding autofilled address:", error);
                
                // API failed, use fallback
                locationSuggestions.innerHTML = '';
                
                // Create fallback items with coordinates from geocoding
                // Try to extract regional or city information from the address
                const addressParts = currentValue.split(',');
                let regionName = '';
                
                // Try to extract region from the address (typically the second-to-last part)
                if (addressParts.length >= 2) {
                    regionName = addressParts[addressParts.length - 2].trim();
                }
                
                // Map of regions to their approximate center coordinates
                const regionCoordinates = {
                    'Sardegna': {lat: "40.0690", lon: "9.0212"},
                    'Sardinia': {lat: "40.0690", lon: "9.0212"},
                    'Sicilia': {lat: "37.5990", lon: "14.0154"},
                    'Sicily': {lat: "37.5990", lon: "14.0154"},
                    'Lombardia': {lat: "45.4785", lon: "9.2195"},
                    'Lombardy': {lat: "45.4785", lon: "9.2195"},
                    'Lazio': {lat: "41.9028", lon: "12.4964"},
                    'Toscana': {lat: "43.7711", lon: "11.2486"},
                    'Tuscany': {lat: "43.7711", lon: "11.2486"},
                    'Campania': {lat: "40.8358", lon: "14.2487"},
                    'Piemonte': {lat: "45.0703", lon: "7.6869"},
                    'Piedmont': {lat: "45.0703", lon: "7.6869"},
                    'Puglia': {lat: "41.1187", lon: "16.8718"},
                    'Apulia': {lat: "41.1187", lon: "16.8718"},
                    'Emilia-Romagna': {lat: "44.4938", lon: "11.3426"},
                    'Calabria': {lat: "39.3087", lon: "16.3158"},
                    'Liguria': {lat: "44.4222", lon: "8.9052"},
                    'Veneto': {lat: "45.4408", lon: "12.3155"},
                    'Italia': {lat: "41.8719", lon: "12.5674"},
                    'Italy': {lat: "41.8719", lon: "12.5674"}
                };
                
                // Find coordinates based on the region in the address
                let coords = {lat: "41.8719", lon: "12.5674"}; // Default to Italy center
                
                // Check if we have a region match - checking parts to avoid false positives
                // Split the address by commas to get components
                const addressComponents = currentValue.split(',').map(part => part.trim());
                
                // First, try to match regions in the second-to-last component (typical for Italian addresses)
                if (addressComponents.length >= 2) {
                    const regionPart = addressComponents[addressComponents.length - 2];
                    for (const [region, coordinates] of Object.entries(regionCoordinates)) {
                        // Check for exact match in address part (not substring)
                        if (regionPart === region || 
                            regionPart.endsWith(` ${region}`) || 
                            regionPart.startsWith(`${region} `)) {
                            coords = coordinates;
                            console.log(`Found exact region match in part "${regionPart}": ${region} -> using coordinates:`, coords);
                            break;
                        }
                    }
                }
                
                // If no match found yet, try more general matching but only for region names, not cities
                // This avoids matching "Via Roma" with "Roma" region
                if (coords.lat === "41.8719") { // If still using default Italy coords
                    const regionRegex = /\b(Sardegna|Sicilia|Lombardia|Lazio|Toscana|Campania|Piemonte|Puglia|Emilia-Romagna|Calabria|Liguria|Veneto)\b/i;
                    const match = currentValue.match(regionRegex);
                    if (match) {
                        const foundRegion = match[0];
                        // Find region in our map (case-insensitive)
                        for (const [region, coordinates] of Object.entries(regionCoordinates)) {
                            if (region.toLowerCase() === foundRegion.toLowerCase()) {
                                coords = coordinates;
                                console.log(`Found region in address: ${region} -> using coordinates:`, coords);
                                break;
                            }
                        }
                    }
                }
                
                const fallbackAddresses = [
                    {
                        display_name: currentValue,
                        lat: coords.lat, // Use region-appropriate coordinates
                        lon: coords.lon
                    }
                ];
                
                // Use fallback data
                latitudeInput.value = fallbackAddresses[0].lat;
                longitudeInput.value = fallbackAddresses[0].lon;
                
                // Also update the query_text hidden field
                if (document.getElementById('query_text')) {
                    document.getElementById('query_text').value = currentValue;
                }
                
                // Store selected location
                selectedLocation = {
                    lat: fallbackAddresses[0].lat,
                    lon: fallbackAddresses[0].lon,
                    displayName: currentValue
                };
                
                // Attiva l'indicatore verde di conferma
                const confirmationIndicator = document.getElementById('location-confirmation');
                if (confirmationIndicator) {
                    confirmationIndicator.classList.add('active');
                }
                
                // Nascondiamo l'indicatore originale
                const indicator = document.getElementById('location-selected-indicator');
                if (indicator) {
                    // indicator.classList.remove('d-none'); // Non mostriamo più questo indicatore
                    
                    const locationNameIndicator = indicator.querySelector('.location-name');
                    if (locationNameIndicator) {
                        locationNameIndicator.textContent = currentValue.substring(0, 25) + 
                            (currentValue.length > 25 ? '...' : '');
                    }
                    
                    const radiusElement = document.getElementById('radius');
                    const radiusValue = radiusElement ? radiusElement.value : '30';
                    const radiusIndicator = indicator.querySelector('.radius-indicator');
                    if (radiusIndicator) {
                        radiusIndicator.textContent = radiusValue + ' km';
                    }
                }
            });
            
            // Update the query_text field with the autofilled value as a fallback
            if (document.getElementById('query_text')) {
                document.getElementById('query_text').value = currentValue;
            }
        }
    }
    
    // Helper to check if the value looks like a complete address
    function isCompleteAddress(text) {
        // Check for common patterns that indicate a complete address
        // Italian address typically has: street, house number, city, province/region
        const hasStreet = /(via|viale|piazza|corso|strada|largo)/i.test(text);
        const hasNumber = /\d+/.test(text);
        const hasCity = text.split(',').length >= 2;
        const hasMultipleParts = text.split(' ').length >= 3;
        
        return (hasStreet && hasNumber && hasCity) || 
               (hasMultipleParts && hasCity) ||
               text.length > 20; // Longer text is more likely to be a complete address
    }
    
    // Setup autofill detection
    // Check right after page load (for page reload with browser-saved values)
    setTimeout(checkForAutofill, 500);
    
    // Check on focus events (browsers often autofill on focus)
    locationInput.addEventListener('focus', function() {
        setTimeout(checkForAutofill, 100);
        // Start aggressive polling when field gets focus
        startAutofillPolling();
    });
    
    // Also check on click, which can trigger autofill on mobile
    locationInput.addEventListener('click', function() {
        setTimeout(checkForAutofill, 100);
        // Start aggressive polling when field gets clicked
        startAutofillPolling();
    });
    
    // Start polling right away for immediate detection
    startAutofillPolling();
    
    // Function to fetch location suggestions from Nominatim
    function fetchSuggestions(query) {
        console.log("fetchSuggestions called with:", query);
        
        if (query.length < 3) {
            console.log("Query too short, not fetching suggestions");
            locationSuggestions.innerHTML = '';
            locationSuggestions.style.display = 'none';
            return;
        }
        
        // Create fallback for API to ensure suggestions always appear
        // even if the external API has rate limits or fails
        const showFallbackData = () => {
            console.log("Showing fallback location data");
            locationSuggestions.innerHTML = '';
            
            // Create fallback items based on query contents
            // Try to detect region information in the query
            let regionName = '';
            
            // Map of regions to their approximate center coordinates
            const regionCoordinates = {
                'Sardegna': {lat: "40.0690", lon: "9.0212"},
                'Sardinia': {lat: "40.0690", lon: "9.0212"},
                'Sicilia': {lat: "37.5990", lon: "14.0154"},
                'Sicily': {lat: "37.5990", lon: "14.0154"},
                'Lombardia': {lat: "45.4785", lon: "9.2195"},
                'Lombardy': {lat: "45.4785", lon: "9.2195"},
                'Milano': {lat: "45.4642", lon: "9.1900"},
                'Milan': {lat: "45.4642", lon: "9.1900"},
                'Roma': {lat: "41.9028", lon: "12.4964"},
                'Rome': {lat: "41.9028", lon: "12.4964"},
                'Napoli': {lat: "40.8518", lon: "14.2681"},
                'Naples': {lat: "40.8518", lon: "14.2681"},
                'Lazio': {lat: "41.9028", lon: "12.4964"},
                'Toscana': {lat: "43.7711", lon: "11.2486"},
                'Tuscany': {lat: "43.7711", lon: "11.2486"},
                'Campania': {lat: "40.8358", lon: "14.2487"},
                'Piemonte': {lat: "45.0703", lon: "7.6869"},
                'Piedmont': {lat: "45.0703", lon: "7.6869"},
                'Puglia': {lat: "41.1187", lon: "16.8718"},
                'Apulia': {lat: "41.1187", lon: "16.8718"},
                'Emilia-Romagna': {lat: "44.4938", lon: "11.3426"},
                'Calabria': {lat: "39.3087", lon: "16.3158"},
                'Liguria': {lat: "44.4222", lon: "8.9052"},
                'Veneto': {lat: "45.4408", lon: "12.3155"},
                'Italia': {lat: "41.8719", lon: "12.5674"},
                'Italy': {lat: "41.8719", lon: "12.5674"}
            };
            
            // Find coordinates based on the query text
            let coords = {lat: "41.8719", lon: "12.5674"}; // Default to Italy center
            
            // Check if we have a region match - checking parts to avoid false positives
            // Split the address by commas to get components
            const addressComponents = query.split(',').map(part => part.trim());
            
            // First, try to match regions in the second-to-last component (typical for Italian addresses)
            if (addressComponents.length >= 2) {
                const regionPart = addressComponents[addressComponents.length - 2];
                for (const [region, coordinates] of Object.entries(regionCoordinates)) {
                    // Check for exact match in address part (not substring)
                    if (regionPart === region || 
                        regionPart.endsWith(` ${region}`) || 
                        regionPart.startsWith(`${region} `)) {
                        coords = coordinates;
                        console.log(`Found exact region match in part "${regionPart}": ${region} -> using coordinates:`, coords);
                        break;
                    }
                }
            }
            
            // If no match found yet, try more general matching but only for region names, not cities
            // This avoids matching "Via Roma" with "Roma" region
            if (coords.lat === "41.8719") { // If still using default Italy coords
                const regionRegex = /\b(Sardegna|Sicilia|Lombardia|Lazio|Toscana|Campania|Piemonte|Puglia|Emilia-Romagna|Calabria|Liguria|Veneto)\b/i;
                const match = query.match(regionRegex);
                if (match) {
                    const foundRegion = match[0];
                    // Find region in our map (case-insensitive)
                    for (const [region, coordinates] of Object.entries(regionCoordinates)) {
                        if (region.toLowerCase() === foundRegion.toLowerCase()) {
                            coords = coordinates;
                            console.log(`Found region in address: ${region} -> using coordinates:`, coords);
                            break;
                        }
                    }
                }
            }
            
            const fallbackAddresses = [
                {
                    display_name: query,
                    lat: coords.lat, // Use region-appropriate coordinates
                    lon: coords.lon
                },
                {
                    display_name: query + " (Centro)",
                    lat: coords.lat, 
                    lon: coords.lon
                }
            ];
            
            fallbackAddresses.forEach(location => {
                const item = document.createElement('div');
                item.className = 'location-suggestion-item';
                item.textContent = location.display_name;
                
                // Store location data
                item.dataset.lat = location.lat;
                item.dataset.lon = location.lon;
                item.dataset.displayName = location.display_name;
                
                item.addEventListener('click', function() {
                    console.log("Fallback suggestion clicked:", this.dataset.displayName);
                    // Set the input value to the selected location name
                    locationInput.value = this.dataset.displayName;
                    
                    // Store coordinates in hidden inputs
                    latitudeInput.value = this.dataset.lat;
                    longitudeInput.value = this.dataset.lon;
                    
                    // Also update the query_text hidden field
                    if (document.getElementById('query_text')) {
                        document.getElementById('query_text').value = this.dataset.displayName;
                    }
                    
                    // Store selected location
                    selectedLocation = {
                        lat: this.dataset.lat,
                        lon: this.dataset.lon,
                        displayName: this.dataset.displayName
                    };
                    
                    // Mostra l'indicatore verde di conferma
                    const confirmationIndicator = document.getElementById('location-confirmation');
                    if (confirmationIndicator) {
                        confirmationIndicator.classList.add('active');
                    }
                    
                    // Nascondiamo l'indicatore originale
                    const indicator = document.getElementById('location-selected-indicator');
                    if (indicator) {
                        // indicator.classList.remove('d-none'); // Non mostriamo più questo indicatore
                        
                        const locationNameIndicator = indicator.querySelector('.location-name');
                        if (locationNameIndicator) {
                            locationNameIndicator.textContent = this.dataset.displayName.substring(0, 25) + 
                                (this.dataset.displayName.length > 25 ? '...' : '');
                        }
                        
                        const radiusElement = document.getElementById('radius');
                        const radiusValue = radiusElement ? radiusElement.value : '30';
                        const radiusIndicator = indicator.querySelector('.radius-indicator');
                        if (radiusIndicator) {
                            radiusIndicator.textContent = radiusValue + ' km';
                        }
                    }
                    
                    // Hide suggestions
                    locationSuggestions.style.display = 'none';
                });
                
                locationSuggestions.appendChild(item);
            });
            
            // Show the suggestions dropdown
            locationSuggestions.style.display = 'block';
            locationInput.classList.remove('loading');
        };
        
        // Show loading indicator
        locationInput.classList.add('loading');
        console.log("Added loading class to input");
        
        // Utilizzare l'API Nominatim per ottenere suggerimenti reali
        
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
        
        console.log("Fetching suggestions from URL:", apiUrl.toString());
        
        fetch(apiUrl.toString(), {
            headers: {
                'User-Agent': 'FindMyCure-Italia/1.0'
            }
        })
        .then(response => response.json())
        .then(data => {
            console.log("Received suggestion data:", data);
            locationSuggestions.innerHTML = '';
            
            if (data.length === 0) {
                console.log("No results found from API, showing fallback");
                // Show fallback suggestions when API returns no results
                showFallbackData();
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
                    
                    // Mostra l'indicatore verde di conferma
                    const confirmationIndicator = document.getElementById('location-confirmation');
                    if (confirmationIndicator) {
                        confirmationIndicator.classList.add('active');
                    }
                    
                    // Nascondiamo l'indicatore originale
                    const indicator = document.getElementById('location-selected-indicator');
                    if (indicator) {
                        // indicator.classList.remove('d-none'); // Non mostriamo più questo indicatore
                        
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
            // If API call fails, use fallback
            showFallbackData();
        });
    }
    
    // Handle input changes (with debounce)
    locationInput.addEventListener('input', function() {
        clearTimeout(typingTimer);
        
        // Clear stored location when user types
        selectedLocation = null;
        latitudeInput.value = '';
        longitudeInput.value = '';
        
        // Hide the green confirmation icon when user types
        const confirmationIndicator = document.getElementById('location-confirmation');
        if (confirmationIndicator) {
            confirmationIndicator.classList.remove('active');
        }
        
        const query = this.value.trim();
        
        // Update last known value to detect autofill
        lastInputValue = query;
        
        // Don't search for single letters or empty queries
        if (query.length < 2) {
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
    
    // Handle blur event (when the field loses focus)
    locationInput.addEventListener('blur', function() {
        // After a short delay to allow for click events on suggestions
        setTimeout(() => {
            const currentValue = locationInput.value.trim();
            
            // If we have a complete address, make sure the query_text is updated
            if (currentValue.length > 15 && isCompleteAddress(currentValue)) {
                console.log('Complete address detected on blur, ensuring query_text is updated');
                if (document.getElementById('query_text')) {
                    document.getElementById('query_text').value = currentValue;
                }
            }
        }, 200);
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
    
    // Removed manual suggestions button - now using automatic detection with polling
    // This works much better with browser/mobile autofill
    
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
    
    // Listen for Enter key press to submit form directly
    locationInput.addEventListener('keydown', function(event) {
        // Enter key pressed
        if (event.key === 'Enter' || event.keyCode === 13) {
            event.preventDefault();
            
            // Update the query_text field
            if (document.getElementById('query_text')) {
                document.getElementById('query_text').value = this.value.trim();
            }
            
            // Click search button
            const searchButton = document.getElementById('searchButton');
            if (searchButton) {
                searchButton.click();
            }
        }
    });
    
    // Apply some initial styling
    locationSuggestions.style.display = 'none';
});