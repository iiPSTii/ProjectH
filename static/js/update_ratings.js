document.addEventListener('DOMContentLoaded', function() {
    // Update all quality score displays with star ratings
    const facilityRatings = document.querySelectorAll('.facility-rating');
    
    facilityRatings.forEach(function(ratingContainer) {
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
        const parentElement = progressBar.parentElement;
        const scoreSpan = parentElement.querySelector('span');
        
        // Create a new container for the stars and score
        const newRatingContainer = document.createElement('div');
        newRatingContainer.className = 'd-flex justify-content-between align-items-center mb-1';
        newRatingContainer.innerHTML = starsHtml;
        
        if (scoreSpan) {
            newRatingContainer.appendChild(scoreSpan.cloneNode(true));
        }
        
        parentElement.replaceWith(newRatingContainer);
    });
});