// main.js - Client-side functionality for FindMyCure Italia

document.addEventListener('DOMContentLoaded', function() {
    // Initialize tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
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

    // Dismiss flash messages after 5 seconds
    setTimeout(function() {
        const alerts = document.querySelectorAll('.alert');
        alerts.forEach(alert => {
            const bsAlert = new bootstrap.Alert(alert);
            bsAlert.close();
        });
    }, 5000);
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

// Find a website for a facility
function findFacilityWebsite(facilityId, buttonElement) {
    // Change button text to indicate loading
    const originalText = buttonElement.innerHTML;
    buttonElement.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Cercando...';
    buttonElement.disabled = true;
    
    // Make AJAX request to find website
    fetch(`/find-website/${facilityId}`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            // Replace the button with the website link
            const websiteContainer = buttonElement.closest('.website-container');
            const linkHtml = `
                <a href="${data.website}" target="_blank" class="website-link text-truncate d-inline-block" 
                   style="max-width: 200px;" data-bs-toggle="tooltip" title="${data.website}">
                   <i class="bi bi-link-45deg"></i> ${data.website}
                </a>
            `;
            websiteContainer.innerHTML = linkHtml;
            
            // Initialize the tooltip
            const tooltipTrigger = websiteContainer.querySelector('[data-bs-toggle="tooltip"]');
            new bootstrap.Tooltip(tooltipTrigger);
            
            // Show success message
            showToast('Successo', data.message, 'success');
        } else {
            // Show error message and reset button
            showToast('Errore', data.error || 'Non è stato possibile trovare un sito web.', 'danger');
            buttonElement.innerHTML = originalText;
            buttonElement.disabled = false;
        }
    })
    .catch(error => {
        console.error('Error finding website:', error);
        showToast('Errore', 'Si è verificato un errore durante la ricerca del sito web.', 'danger');
        buttonElement.innerHTML = originalText;
        buttonElement.disabled = false;
    });
}

// Show a toast notification
function showToast(title, message, type = 'info') {
    // Create toast container if it doesn't exist
    let toastContainer = document.querySelector('.toast-container');
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.className = 'toast-container position-fixed bottom-0 end-0 p-3';
        document.body.appendChild(toastContainer);
    }
    
    // Create toast element
    const toastId = 'toast-' + Date.now();
    const toastHtml = `
        <div id="${toastId}" class="toast" role="alert" aria-live="assertive" aria-atomic="true">
            <div class="toast-header bg-${type} text-white">
                <strong class="me-auto">${title}</strong>
                <button type="button" class="btn-close btn-close-white" data-bs-dismiss="toast" aria-label="Close"></button>
            </div>
            <div class="toast-body">
                ${message}
            </div>
        </div>
    `;
    
    // Add toast to container
    toastContainer.innerHTML += toastHtml;
    
    // Initialize and show the toast
    const toastElement = document.getElementById(toastId);
    const toast = new bootstrap.Toast(toastElement, { delay: 5000 });
    toast.show();
}
