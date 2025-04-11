/**
 * Mappa di densità delle strutture sanitarie
 * 
 * Questo script gestisce la visualizzazione e l'interazione con la mappa di calore
 * che mostra la densità delle strutture sanitarie in Italia.
 */
 
document.addEventListener('DOMContentLoaded', function() {
    // Centro della mappa sull'Italia
    const ITALY_CENTER = [42.504154, 12.646361];
    const DEFAULT_ZOOM = 6;
    const MAX_HEAT_RADIUS = 30;
    
    // Riferimenti agli elementi DOM
    const mapElement = document.getElementById('map');
    const loadingOverlay = document.getElementById('loading-overlay');
    const regionFilter = document.getElementById('region-filter');
    const specialtyFilter = document.getElementById('specialty-filter');
    const minQualityFilter = document.getElementById('min-quality-filter');
    const applyFiltersBtn = document.getElementById('apply-filters');
    const resetFiltersBtn = document.getElementById('reset-filters');
    
    // Stato dell'applicazione
    let map = null;
    let heatLayer = null;
    let markersLayer = null;
    let facilities = [];
    let currentFilters = {
        region: '',
        specialty: '',
        minQuality: 0
    };
    
    // Inizializza la mappa
    function initMap() {
        // Crea la mappa Leaflet
        map = L.map('map').setView(ITALY_CENTER, DEFAULT_ZOOM);
        
        // Aggiungi il layer della mappa (OpenStreetMap)
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }).addTo(map);
        
        // Crea i layer per heatmap e markers (inizialmente vuoti)
        markersLayer = L.layerGroup().addTo(map);
        
        // Carica i dati iniziali
        loadFacilitiesData();
        
        // Aggiungi gestori eventi per i filtri
        setupEventListeners();
    }
    
    // Carica i dati delle strutture dall'API
    function loadFacilitiesData() {
        // Mostra l'overlay di caricamento
        showLoading(true);
        
        // Costruisci l'URL con i parametri di filtro
        const params = new URLSearchParams();
        if (currentFilters.region) params.append('region', currentFilters.region);
        if (currentFilters.specialty) params.append('specialty', currentFilters.specialty);
        if (currentFilters.minQuality > 0) params.append('min_quality', currentFilters.minQuality);
        
        // Effettua la richiesta API
        fetch(`/api/facilities-data?${params.toString()}`)
            .then(response => response.json())
            .then(data => {
                facilities = data.facilities;
                updateHeatmap();
                showLoading(false);
            })
            .catch(error => {
                console.error('Errore nel caricamento dei dati:', error);
                showLoading(false);
                alert('Si è verificato un errore nel caricamento dei dati. Riprova più tardi.');
            });
    }
    
    // Aggiorna la heatmap con i dati delle strutture
    function updateHeatmap() {
        // Rimuovi il layer della heatmap esistente se presente
        if (heatLayer) {
            map.removeLayer(heatLayer);
        }
        
        // Prepara i dati per la heatmap
        const heatData = facilities.map(facility => {
            // Usa il rating come intensità (se disponibile)
            const intensity = facility.quality_rating || 1;  // manteniamo quality_rating perché l'API ora restituisce il campo con questo nome
            return [facility.latitude, facility.longitude, intensity];
        });
        
        // Crea un nuovo layer heatmap
        heatLayer = L.heatLayer(heatData, {
            radius: getHeatmapRadius(),
            blur: 15,
            maxZoom: 10,
            max: 5,  // Il valore massimo di intensità
            gradient: {
                0.2: 'blue',
                0.4: 'lime',
                0.6: 'yellow',
                0.8: 'orange',
                1.0: 'red'
            }
        }).addTo(map);
        
        // Aggiungi anche i marker al click sulla mappa
        setupMapClickHandler();
    }
    
    // Calcola il raggio adeguato per la heatmap in base al livello di zoom
    function getHeatmapRadius() {
        const zoom = map.getZoom();
        // Riduci il raggio quando siamo zoomati molto
        return Math.max(8, MAX_HEAT_RADIUS - (zoom - DEFAULT_ZOOM));
    }
    
    // Configura il gestore degli eventi per il click sulla mappa
    function setupMapClickHandler() {
        map.on('click', function(e) {
            const clickLat = e.latlng.lat;
            const clickLng = e.latlng.lng;
            const radius = 30; // Raggio di ricerca in km
            
            // Trova le strutture nel raggio del click
            const nearbyFacilities = facilities.filter(facility => {
                const distance = calculateDistance(
                    clickLat, clickLng,
                    facility.latitude, facility.longitude
                );
                return distance <= radius;
            });
            
            // Ordina per vicinanza al punto di click
            nearbyFacilities.sort((a, b) => {
                const distA = calculateDistance(clickLat, clickLng, a.latitude, a.longitude);
                const distB = calculateDistance(clickLat, clickLng, b.latitude, b.longitude);
                return distA - distB;
            });
            
            // Aggiorna i marker sulla mappa
            updateMarkers(nearbyFacilities, clickLat, clickLng);
        });
    }
    
    // Aggiorna i marker sulla mappa
    function updateMarkers(nearbyFacilities, clickLat, clickLng) {
        // Pulisci i marker esistenti
        markersLayer.clearLayers();
        
        // Se non ci sono strutture vicine, mostra un messaggio
        if (nearbyFacilities.length === 0) {
            L.marker([clickLat, clickLng])
                .bindPopup('<strong>Nessuna struttura sanitaria in quest\'area</strong>')
                .addTo(markersLayer)
                .openPopup();
            return;
        }
        
        // Aggiungi un marker per ogni struttura vicina
        nearbyFacilities.forEach(facility => {
            const marker = L.marker([facility.latitude, facility.longitude]);
            
            // Calcola la distanza
            const distance = calculateDistance(
                clickLat, clickLng,
                facility.latitude, facility.longitude
            );
            
            // Prepara il contenuto del popup
            let popupContent = `
                <div class="popup-content">
                    <h5>${facility.name}</h5>
                    <p><i class="fas fa-map-marker-alt"></i> ${facility.address}, ${facility.city}</p>
                    <p><i class="fas fa-star"></i> Qualità: ${formatQuality(facility.quality_rating)}</p>
                    <p><i class="fas fa-route"></i> Distanza: ${distance.toFixed(1)} km</p>
            `;
            
            // Aggiungi il rating della specialità se disponibile
            if (facility.specialty_rating) {
                popupContent += `<p><i class="fas fa-stethoscope"></i> Rating ${currentFilters.specialty}: ${formatQuality(facility.specialty_rating)}</p>`;
            }
            
            // Aggiungi link alla pagina della struttura (se implementata)
            popupContent += `<a href="/search?query_text=${encodeURIComponent(facility.name)}" class="btn btn-sm btn-primary mt-2">Dettagli struttura</a>`;
            popupContent += `</div>`;
            
            // Aggiungi il popup al marker
            marker.bindPopup(popupContent);
            
            // Aggiungi il marker al layer
            marker.addTo(markersLayer);
        });
        
        // Apri il popup del primo marker (quello più vicino)
        const markers = Object.values(markersLayer._layers);
        if (markers.length > 0) {
            markers[0].openPopup();
        }
    }
    
    // Formatta il valore di qualità (1-5) in stelle
    function formatQuality(quality) {
        if (!quality) return 'N/A';
        
        const fullStars = Math.floor(quality);
        const halfStar = quality % 1 >= 0.5;
        let starsHtml = '';
        
        // Aggiungi stelle piene
        for (let i = 0; i < fullStars; i++) {
            starsHtml += '<i class="fas fa-star text-warning"></i>';
        }
        
        // Aggiungi mezza stella se necessario
        if (halfStar) {
            starsHtml += '<i class="fas fa-star-half-alt text-warning"></i>';
        }
        
        // Aggiungi stelle vuote
        const emptyStars = 5 - fullStars - (halfStar ? 1 : 0);
        for (let i = 0; i < emptyStars; i++) {
            starsHtml += '<i class="far fa-star text-warning"></i>';
        }
        
        return starsHtml;
    }
    
    // Calcola la distanza tra due punti (formula di Haversine)
    function calculateDistance(lat1, lon1, lat2, lon2) {
        const R = 6371; // Raggio della Terra in km
        const dLat = toRadians(lat2 - lat1);
        const dLon = toRadians(lon2 - lon1);
        const a = 
            Math.sin(dLat/2) * Math.sin(dLat/2) +
            Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * 
            Math.sin(dLon/2) * Math.sin(dLon/2);
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c;
    }
    
    // Converte gradi in radianti
    function toRadians(degrees) {
        return degrees * Math.PI / 180;
    }
    
    // Mostra/nascondi l'overlay di caricamento
    function showLoading(show) {
        loadingOverlay.style.display = show ? 'flex' : 'none';
    }
    
    // Configura gli event listener per i controlli
    function setupEventListeners() {
        // Event listener per il pulsante Applica filtri
        applyFiltersBtn.addEventListener('click', function() {
            // Aggiorna i filtri correnti
            currentFilters.region = regionFilter.value;
            currentFilters.specialty = specialtyFilter.value;
            currentFilters.minQuality = parseFloat(minQualityFilter.value);
            
            // Carica i nuovi dati con i filtri applicati
            loadFacilitiesData();
        });
        
        // Event listener per il pulsante Reset
        resetFiltersBtn.addEventListener('click', function() {
            // Resetta i valori dei filtri
            regionFilter.value = '';
            specialtyFilter.value = '';
            minQualityFilter.value = '0';
            
            // Resetta i filtri correnti
            currentFilters.region = '';
            currentFilters.specialty = '';
            currentFilters.minQuality = 0;
            
            // Carica i dati senza filtri
            loadFacilitiesData();
        });
        
        // Aggiorna il raggio della heatmap quando la mappa viene zoomata
        map.on('zoomend', function() {
            if (heatLayer) {
                // Rimuovi il layer esistente
                map.removeLayer(heatLayer);
                
                // Ricrea il layer con il nuovo raggio
                const heatData = facilities.map(facility => {
                    const intensity = facility.quality_rating || 1;  // manteniamo quality_rating perché l'API ora restituisce il campo con questo nome
                    return [facility.latitude, facility.longitude, intensity];
                });
                
                heatLayer = L.heatLayer(heatData, {
                    radius: getHeatmapRadius(),
                    blur: 15,
                    maxZoom: 10,
                    max: 5,
                    gradient: {
                        0.2: 'blue',
                        0.4: 'lime',
                        0.6: 'yellow',
                        0.8: 'orange',
                        1.0: 'red'
                    }
                }).addTo(map);
            }
        });
    }
    
    // Inizializza la mappa
    initMap();
});