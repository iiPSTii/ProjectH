// main.js - Client-side functionality for FindMyCure Italia

document.addEventListener('DOMContentLoaded', function() {
    // Initialize tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
    
    // Let Bootstrap handle modals by itself

    // Handle quality slider on the search form
    const qualitySlider = document.getElementById('min_quality');
    if (qualitySlider) {
        qualitySlider.addEventListener('input', function() {
            updateQualityValue(this.value);
        });
    }
    
    // IMPORTANT: Reset search button state on page load
    // This fixes the issue with the button staying in loading state when navigating back
    const searchButton = document.getElementById('searchButton');
    if (searchButton) {
        // Reset any stored disabled state and restore original text
        searchButton.disabled = false;
        searchButton.innerHTML = '<i class="fas fa-search me-2"></i> Cerca Strutture';
    }
    
    // Add loading spinner to search form
    const searchForm = document.getElementById('searchForm');
    if (searchForm) {
        // Store original button HTML to restore it later if needed
        const originalButtonHTML = searchButton ? searchButton.innerHTML : '<i class="fas fa-search me-2"></i> Cerca Strutture';
        
        searchForm.addEventListener('submit', function() {
            const searchButton = document.getElementById('searchButton');
            if (searchButton) {
                // Disable the button to prevent multiple submissions
                searchButton.disabled = true;
                
                // Change button text and add spinner
                searchButton.innerHTML = '<i class="fas fa-search me-2"></i> Ricerca in corso... <span class="spinner-border spinner-border-sm ms-2" role="status" aria-hidden="true"></span>';
                
                // Save the form submission time to localStorage
                localStorage.setItem('lastSearchTime', Date.now());
            }
            
            // Allow the form to submit
            return true;
        });
        
        // Reset the button if the browser's back button was used to return to this page
        window.addEventListener('pageshow', function(event) {
            // This will fire when the page is shown, including when coming back via browser back button
            if (event.persisted || window.performance && window.performance.navigation.type === 2) {
                // Reset the search button state
                const searchButton = document.getElementById('searchButton');
                if (searchButton) {
                    searchButton.disabled = false;
                    searchButton.innerHTML = originalButtonHTML;
                }
            }
        });
    }

    // Initialize any collapsible elements
    const collapseElementList = [].slice.call(document.querySelectorAll('.collapse'));
    collapseElementList.map(function(collapseEl) {
        return new bootstrap.Collapse(collapseEl, {
            toggle: false
        });
    });

    // We've removed the auto-dismiss code for alerts
    // All informational elements now use permanent cards instead of alerts
    
    // Grid/List view toggle
    const viewGridBtn = document.getElementById('viewGrid');
    const viewListBtn = document.getElementById('viewList');
    const facilitiesGrid = document.getElementById('facilitiesGrid');
    const facilitiesList = document.getElementById('facilitiesList');
    
    if (viewGridBtn && viewListBtn && facilitiesGrid && facilitiesList) {
        // Default view is grid
        facilitiesGrid.style.display = 'flex';
        facilitiesList.style.display = 'none';
        viewGridBtn.classList.add('active');
        
        // Switch to grid view
        viewGridBtn.addEventListener('click', function() {
            facilitiesGrid.style.display = 'flex';
            facilitiesList.style.display = 'none';
            viewGridBtn.classList.add('active');
            viewListBtn.classList.remove('active');
        });
        
        // Switch to list view
        viewListBtn.addEventListener('click', function() {
            facilitiesGrid.style.display = 'none';
            facilitiesList.style.display = 'block';
            viewListBtn.classList.add('active');
            viewGridBtn.classList.remove('active');
        });
    }
});

// Update quality slider value display
function updateQualityValue(val) {
    const qualityValueElement = document.getElementById('qualityValue');
    if (qualityValueElement) {
        if (val == 0) {
            qualityValueElement.textContent = 'Qualsiasi';
        } else {
            qualityValueElement.textContent = val + '/5';
        }
    }
}

// Handle specific query transfer to main search field
function useSpecificQuery() {
    const specificQuery = document.getElementById('specific_query');
    const mainQueryField = document.getElementById('query_text');
    
    if (specificQuery && mainQueryField && specificQuery.value.trim() !== '') {
        // Transfer the specific query value to the main search field
        mainQueryField.value = specificQuery.value;
        
        // Collapse the specific search section
        const specificSearchSection = document.getElementById('specificSearch');
        if (specificSearchSection) {
            const bsCollapse = bootstrap.Collapse.getInstance(specificSearchSection);
            if (bsCollapse) {
                bsCollapse.hide();
            }
        }
        
        // Focus on the main search button
        const searchButton = document.getElementById('searchButton');
        if (searchButton) {
            searchButton.focus();
            // Optional: Add a visual indicator that the query was transferred
            searchButton.classList.add('btn-success');
            setTimeout(() => {
                searchButton.classList.remove('btn-success');
                searchButton.classList.add('btn-primary');
            }, 1000);
        }
    }
}

// Handle applying location search from address field
function applyLocationSearch() {
    const locationInput = document.getElementById('location-input');
    const queryTextField = document.getElementById('query_text');
    const searchButton = document.getElementById('searchButton');
    
    if (locationInput && locationInput.value.trim() !== '') {
        // Make sure the query_text field has the location value
        if (queryTextField) {
            queryTextField.value = locationInput.value;
        }
        
        // Add a visual indicator that the location search is active
        const searchButton = document.getElementById('searchButton');
        if (searchButton) {
            searchButton.focus();
            // Visual feedback - change the main button to show it will perform a location search
            searchButton.innerHTML = '<i class="fas fa-map-marker-alt me-2"></i> Cerca Strutture Vicino a ' + 
                                    '<span class="badge bg-info">' + locationInput.value.substring(0, 15) + 
                                    (locationInput.value.length > 15 ? '...' : '') + '</span>';
        }
        
        // Collapse any open filter sections
        const filterOptions = document.getElementById('filterOptions');
        if (filterOptions && filterOptions.classList.contains('show')) {
            const bsCollapse = bootstrap.Collapse.getInstance(filterOptions);
            if (bsCollapse) {
                bsCollapse.hide();
            }
        }
        
        // Scroll to the main search button for better UX
        if (searchButton) {
            searchButton.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    }
}
