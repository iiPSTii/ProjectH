document.addEventListener('DOMContentLoaded', function() {
    // Update only the general quality score displays with star ratings
    // This specifically targets the section with "Qualità Generale:" heading
    
    document.querySelectorAll('h6.mb-1').forEach(function(heading) {
        // Find only the "Qualità Generale" headings
        if (heading.textContent.trim() === 'Qualità Generale:') {
            // Find the closest facility rating container
            const ratingContainer = heading.nextElementSibling;
            if (!ratingContainer || !ratingContainer.classList.contains('facility-rating')) return;
            
            const progressBar = ratingContainer.querySelector('.progress');
            if (!progressBar) return;
            
            // Get the current score
            const progressValue = progressBar.querySelector('.progress-bar');
            if (!progressValue) return;
            
            const widthStr = progressValue.style.width;
            const percentValue = parseFloat(widthStr);
            const score = percentValue / 20; // Convert percent to 5-star scale
            
            // Create star rating HTML
            const scoreInteger = Math.floor(score);
            const decimal = (score - scoreInteger) * 10;
            
            let starsHtml = '<div class="star-rating">';
            for (let i = 0; i < 5; i++) {
                if (i < scoreInteger) {
                    starsHtml += '<i class="fas fa-star"></i>';
                } else if (i === scoreInteger && decimal >= 5) {
                    starsHtml += '<i class="fas fa-star-half-alt"></i>';
                } else {
                    starsHtml += '<i class="far fa-star"></i>';
                }
            }
            starsHtml += '</div>';
            
            // Replace progress bar with stars
            const parentWrapper = progressBar.closest('.d-flex');
            if (!parentWrapper) return;
            
            const scoreSpan = parentWrapper.querySelector('span');
            
            // Create a new container for the stars and score
            const newRatingContainer = document.createElement('div');
            newRatingContainer.className = 'd-flex justify-content-between align-items-center mb-1';
            newRatingContainer.innerHTML = starsHtml;
            
            if (scoreSpan) {
                newRatingContainer.appendChild(scoreSpan.cloneNode(true));
            }
            
            parentWrapper.replaceWith(newRatingContainer);
        }
    });
});