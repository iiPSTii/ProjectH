// main.js - Client-side functionality for FindMyCure Italia

document.addEventListener('DOMContentLoaded', function() {
    // Initialize tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
    
    // Initialize modals
    const modalTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="modal"]'));
    modalTriggerList.map(function(modalTriggerEl) {
        modalTriggerEl.addEventListener('click', function(event) {
            event.preventDefault();
            const targetModal = document.querySelector(this.getAttribute('data-bs-target'));
            if (targetModal) {
                const modal = new bootstrap.Modal(targetModal);
                modal.show();
            }
        });
    });

    // Handle quality slider on the search form
    const qualitySlider = document.getElementById('min_quality');
    if (qualitySlider) {
        qualitySlider.addEventListener('input', function() {
            updateQualityValue(this.value);
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
