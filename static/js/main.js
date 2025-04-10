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
    
    // Add loading spinner to search form
    const searchForm = document.getElementById('searchForm');
    if (searchForm) {
        searchForm.addEventListener('submit', function() {
            const searchButton = document.getElementById('searchButton');
            if (searchButton) {
                // Disable the button to prevent multiple submissions
                searchButton.disabled = true;
                
                // Change button text and add spinner
                searchButton.innerHTML = '<i class="fas fa-search me-2"></i> Ricerca in corso... <span class="spinner-border spinner-border-sm ms-2" role="status" aria-hidden="true"></span>';
            }
            
            // Allow the form to submit
            return true;
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
