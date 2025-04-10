import os
import logging
from flask import Flask, render_template, request, jsonify, flash, redirect, url_for, send_file
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase
from werkzeug.middleware.proxy_fix import ProxyFix
from medical_mapping import map_query_to_specialties
from medical_professionals import map_profession_to_specialties, PROFESSION_TO_SPECIALTY_MAP
from location_mapping import detect_location_in_query
from geocoding import is_address_query, extract_address_part, find_facilities_near_address

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Define SQLAlchemy base
class Base(DeclarativeBase):
    pass

# Initialize SQLAlchemy
db = SQLAlchemy(model_class=Base)

# Create Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev_secret_key")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Configure the database
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL", "sqlite:///medical_facilities.db")
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_recycle": 300,
    "pool_pre_ping": True,
}
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# Initialize the app with the extension
db.init_app(app)

# Import routes after app initialization to avoid circular imports
with app.app_context():
    # Import models and create tables
    from models import MedicalFacility, Specialty, FacilitySpecialty, Region, DatabaseStatus
    db.create_all()

    # Import and register route functions
    from data_loader import load_data, get_regions, get_specialties, normalize_specialty

    # Check database status
    def get_database_status():
        """Get the current database status"""
        try:
            status = DatabaseStatus.get_status()
            if status:
                logger.info(f"Database status: {status.status} (Last updated: {status.last_updated})")
                return status
            else:
                logger.warning("No database status found. Database may not be initialized.")
                return None
        except Exception as e:
            logger.error(f"Error getting database status: {str(e)}")
            return None

    @app.route('/')
    def index():
        regions = get_regions()
        specialties = get_specialties()
        db_status = get_database_status()
        return render_template('index.html', regions=regions, specialties=specialties, db_status=db_status)

    @app.route('/search')
    def search():
        # Get search parameters
        specialty = request.args.get('specialty', '')
        region = request.args.get('region', '')
        min_quality = request.args.get('min_quality', 0, type=float)
        query_text = request.args.get('query_text', '')
        sort_by = request.args.get('sort_by', 'quality_desc')  # Default sort by quality descending

        # Process the search query to extract location information
        detected_location = None
        original_query = query_text
        is_address_search = False
        address_search_results = None

        if query_text:
            # Prioritize address/location search mode for new search interface
            # Now we treat any query from the main search field as a potential address or city name
            logger.debug(f"Treating query as potential address/location: '{query_text}'")
            is_address_search = True
            
            # Extract the address part from the query if it contains other terms
            address_part = extract_address_part(query_text)
            logger.debug(f"Extracted address part: '{address_part}'")
            
            # Get all facilities to find ones near this address
            all_facilities = db.session.query(MedicalFacility).all()
            
            # Find facilities near the specified address
            # Increased max distance to 30km to get more results
            address_search_results = find_facilities_near_address(address_part, all_facilities, max_distance=30.0)
            
            if address_search_results and address_search_results.get('facilities'):
                logger.debug(f"Found {len(address_search_results['facilities'])} facilities near address: '{query_text}'")
                # Get the detected location display name
                search_location = address_search_results.get('search_location', {})
                detected_location = search_location.get('display_name', query_text)
            else:
                logger.debug(f"No facilities found near address: '{query_text}'")
                # If no facilities found near address, fall back to regular search
                is_address_search = False
            
            # If not an address search or address search found no results, try regular search
            if not is_address_search:
                # Try to detect location/city references in the query
                cleaned_query, detected_region = detect_location_in_query(query_text)

                if detected_region:
                    logger.debug(f"Detected location in query: '{query_text}' -> region: '{detected_region}'")
                    detected_location = detected_region

                    # Only update the query text if we found location
                    if cleaned_query != query_text:
                        if cleaned_query.strip():
                            query_text = cleaned_query
                        else:
                            # If the query was fully consumed by the location detection,
                            # set query_text to empty to avoid duplicate filtering
                            logger.debug(f"Query was fully consumed by location detection: '{original_query}' -> '{detected_region}'")
                            query_text = ""

                    # If no region was specified in the form, use the detected one
                    if not region:
                        region = detected_region

        logger.debug(f"Search params: specialty={specialty}, region={region}, min_quality={min_quality}, query_text={query_text}")

        # Get database status
        db_status = get_database_status()

        # Build the query
        query = db.session.query(MedicalFacility)

        # Apply specialty filter if provided by form
        if specialty:
            # Get equivalent specialties from our mapping system
            from specialty_mapping import get_equivalent_specialties
            equivalent_specialties = get_equivalent_specialties(specialty)
            
            # If we have equivalent specialties, use them
            if equivalent_specialties:
                query = query.join(MedicalFacility.specialties).join(FacilitySpecialty.specialty).filter(
                    Specialty.name.in_(equivalent_specialties)
                )
                logger.debug(f"Using specialty mapping for '{specialty}': {equivalent_specialties}")
            # Otherwise fall back to the original method
            else:
                normalized_specialty = normalize_specialty(specialty)
                query = query.join(MedicalFacility.specialties).join(FacilitySpecialty.specialty).filter(
                    Specialty.name.ilike(f'%{normalized_specialty}%')
                )
                logger.debug(f"No specialty mapping for '{specialty}', using normalized: {normalized_specialty}")
                
            # Check if we'll get any results with this query
            preliminary_count = query.count()
            if preliminary_count == 0:
                logger.debug(f"No results found for specialty '{specialty}', using fallback to common specialties")
                # Reset query and join with specialties
                query = db.session.query(MedicalFacility)
                query = query.join(MedicalFacility.specialties).join(FacilitySpecialty.specialty)
                
                # If region is specified, keep that filter
                if region:
                    query = query.join(MedicalFacility.region, isouter=True).filter(Region.name.ilike(f'%{region}%'))
                
                # Filter by the most common specialties to ensure we get results
                fallback_specialties = ['Medicina Generale', 'Medicina Interna', 'Chirurgia Generale']
                query = query.filter(Specialty.name.in_(fallback_specialties))
                logger.debug(f"Using fallback specialties: {fallback_specialties}")
            
            specialty_filter_applied = True
        else:
            specialty_filter_applied = False

        # Apply region filter if provided
        if region:
            query = query.join(MedicalFacility.region, isouter=True).filter(Region.name.ilike(f'%{region}%'))

        # Apply quality filter if provided
        if min_quality is not None and min_quality > 0:
            query = query.filter(MedicalFacility.quality_score >= min_quality)

        # Apply text search if provided
        mapped_specialties = []
        if query_text:
            # First, try to map the query to medical profession terms
            profession_specialties = map_profession_to_specialties(query_text)

            # If no medical profession mapping found, try medical conditions
            if not profession_specialties:
                condition_specialties = map_query_to_specialties(query_text)
                mapped_specialties = condition_specialties
            else:
                mapped_specialties = profession_specialties

            logger.debug(f"Mapped query '{query_text}' to specialties: {mapped_specialties}")

            # Create a copy of the query before applying specialty filters
            base_query = query
            specialty_search_applied = False

            # If not already filtered by specialty form field and we have mapped specialties
            if not specialty_filter_applied and mapped_specialties:
                specialty_search_applied = True
                
                # For each specialty from mapping, expand it to include equivalent specialties
                from specialty_mapping import get_equivalent_specialties
                expanded_specialties = []
                for s in mapped_specialties:
                    equiv_specs = get_equivalent_specialties(s)
                    if equiv_specs:
                        expanded_specialties.extend(equiv_specs)
                    else:
                        expanded_specialties.append(s)
                        
                logger.debug(f"Expanded mapped specialties: {mapped_specialties} -> {expanded_specialties}")
                
                query = query.join(MedicalFacility.specialties).join(FacilitySpecialty.specialty).filter(
                    Specialty.name.in_(expanded_specialties)
                )

            # Special case for "ospedale [region]" patterns or when query is empty but region is set
            # and original query had "ospedale"
            if ('ospedale' in query_text.lower() and region) or (query_text == "" and region and 'ospedale' in original_query.lower()):
                # This will find all hospitals in the specified region
                search_term = "%ospedale%"
                query = query.filter(db.func.lower(MedicalFacility.name).like(search_term))
                specialty_search_applied = True
                # For logging and user interface clarity
                if query_text == "":
                    logger.debug(f"Empty query_text with 'ospedale' in original query '{original_query}', showing all hospitals in '{region}'")
                    mapped_specialties = ["Tutti gli ospedali della regione"]
            # Special case for "[profession] [city]" patterns (e.g., "oncologo trieste")
            elif any(p in query_text.lower() for p in PROFESSION_TO_SPECIALTY_MAP.keys()) and region:
                # Get the profession term
                prof_term = None
                for term in PROFESSION_TO_SPECIALTY_MAP.keys():
                    if term in query_text.lower():
                        prof_term = term
                        break
                
                if prof_term:
                    # Find specialties associated with this profession
                    specialties_to_search = PROFESSION_TO_SPECIALTY_MAP[prof_term]
                    logger.debug(f"Found profession term '{prof_term}' mapping to: {specialties_to_search}")
                    
                    # Expand the specialties using our macrocategory mapping
                    from specialty_mapping import get_equivalent_specialties
                    expanded_specialties = []
                    for s in specialties_to_search:
                        equiv_specs = get_equivalent_specialties(s)
                        if equiv_specs:
                            expanded_specialties.extend(equiv_specs)
                        else:
                            expanded_specialties.append(s)
                    
                    logger.debug(f"Expanded profession specialties: {specialties_to_search} -> {expanded_specialties}")
                    
                    # Join with specialties tables
                    query = query.join(MedicalFacility.specialties).join(FacilitySpecialty.specialty)
                    
                    # Filter by these specialties
                    query = query.filter(Specialty.name.in_(expanded_specialties))
                    specialty_search_applied = True
            # If no medical mapping found or specialty already filtered, do regular text search
            elif not specialty_search_applied or specialty_filter_applied:
                # Search in facility name, address, specialties, and conditions
                search_term = f"%{query_text.lower()}%"

                # Join with specialties only if not already joined
                if not specialty_filter_applied:
                    query = query.outerjoin(MedicalFacility.specialties).outerjoin(FacilitySpecialty.specialty)

                # Search in facility name, facility type, address, city or specialty name
                query = query.filter(
                    db.or_(
                        db.func.lower(MedicalFacility.name).like(search_term),
                        db.func.lower(MedicalFacility.facility_type).like(search_term),
                        db.func.lower(MedicalFacility.address).like(search_term),
                        db.func.lower(MedicalFacility.city).like(search_term),
                        db.func.lower(Specialty.name).like(search_term)
                    )
                )

            # Execute query to see if we found any results
            facilities = query.all()

            # If no facilities found and we've tried specialty mapping, fall back to general search
            if len(facilities) == 0 and specialty_search_applied:
                logger.debug(f"No facilities found with specialty mapping, falling back to broader search")

                # We need a fresh query with original filters (region, quality) but not the specialty mapping
                query = base_query

                # Get general medical specialties to try as fallbacks
                general_specialties = ['Medicina Generale', 'Medicina Interna', 'Ortopedia']

                # Join specialties tables
                query = query.join(MedicalFacility.specialties).join(FacilitySpecialty.specialty)

                # Filter by these general specialties
                query = query.filter(
                    db.or_(
                        Specialty.name.in_(general_specialties),
                        db.func.lower(MedicalFacility.name).like(f"%{query_text.lower()}%"),
                        db.func.lower(MedicalFacility.city).like(f"%{query_text.lower()}%")
                    )
                )

                # Add a message to indicate we're showing broader results
                if not mapped_specialties:
                    # If we couldn't map the query at all, add general mapping
                    mapped_specialties = general_specialties
                else:
                    # If we had some mappings but found no facilities, add these general ones
                    mapped_specialties.extend(general_specialties)

                # Re-execute query to get facilities
                facilities = query.all()
                logger.debug(f"Fallback search found {len(facilities)} facilities")
            else:
                # We already have facilities from the first query
                logger.debug(f"Primary search found {len(facilities)} facilities")
        else:
            # No text search, just execute the query with existing filters
            facilities = query.all()
            logger.debug(f"Basic filter search found {len(facilities)} facilities")

        # Check if we're using address search results
        if is_address_search and address_search_results and address_search_results.get('facilities'):
            # Get the facilities from our address search
            facilities = address_search_results['facilities']
            search_location = address_search_results['search_location']
            
            # Add distance information for display
            mapped_specialties = [f"Strutture entro 30 km da {search_location.get('display_name', query_text)}"]
            
            # Check if user wants to sort by something other than distance
            if sort_by != 'distance':
                logger.debug(f"Resorting address search results by {sort_by} instead of distance")
                
                # Use the same sorting functions as for normal searches
                sorting_functions = {
                    'quality_desc': lambda x: (x.quality_score if x.quality_score is not None else -1) * -1,
                    'quality_asc': lambda x: x.quality_score if x.quality_score is not None else float('inf'),
                    'name_asc': lambda x: x.name.lower(),
                    'name_desc': lambda x: x.name.lower(),
                    'city_asc': lambda x: (x.city or '').lower(),
                    'city_desc': lambda x: (x.city or '').lower(),
                    'distance': lambda x: x.distance if hasattr(x, 'distance') else float('inf')  # Keep original distance sort
                }
                
                reverse_sort = sort_by.endswith('_desc') and sort_by != 'quality_desc'
                sort_function = sorting_functions.get(sort_by, sorting_functions['distance'])
                facilities = sorted(facilities, key=sort_function, reverse=reverse_sort)
                logger.debug(f"Re-sorted facilities by {sort_by}")
            else:
                logger.debug(f"Using address search results with original distance sorting")
            
            # Add is_address_search flag to indicate this is an address search
            return render_template('results_stars_only.html', facilities=facilities, db_status=db_status, search_params={
                'specialty': specialty,
                'region': region,
                'min_quality': min_quality,
                'query_text': query_text,
                'original_query': original_query,
                'detected_location': detected_location,
                'mapped_specialties': mapped_specialties,
                'sort_by': sort_by,  # Pass the actual sort_by parameter instead of hardcoding 'distance'
                'is_address_search': True,
                'search_location': search_location
            })
        else:
            # Special case for address searches that found no nearby facilities
            # but did successfully recognize a location
            if len(facilities) == 0 and is_address_query(query_text) and not is_address_search:
                logger.debug(f"Address search failed to find nearby facilities, showing all in detected region: {region}")
                
                # If we've detected a region from the address query and have no results,
                # show all facilities in that region without other filters
                if region:
                    # Clear query and create a fresh one with just the region filter
                    query = db.session.query(MedicalFacility)
                    query = query.join(MedicalFacility.region).filter(Region.name.ilike(f'%{region}%'))
                    facilities = query.all()
                    
                    # Add message about showing all facilities in region
                    mapped_specialties = [f"Tutte le strutture nella regione {region}"]
                    logger.debug(f"Showing all {len(facilities)} facilities in region {region}")
            
            # Regular search results - sort the facilities based on the sort_by parameter
            sorting_functions = {
                'quality_desc': lambda x: (x.quality_score if x.quality_score is not None else -1) * -1,  # Default
                'quality_asc': lambda x: x.quality_score if x.quality_score is not None else float('inf'),
                'name_asc': lambda x: x.name.lower(),
                'name_desc': lambda x: x.name.lower(),
                'city_asc': lambda x: (x.city or '').lower(),
                'city_desc': lambda x: (x.city or '').lower(),
            }

            reverse_sort = sort_by.endswith('_desc') and sort_by != 'quality_desc'

            # If sort_by is not in our mapping, default to quality descending
            sort_function = sorting_functions.get(sort_by, sorting_functions['quality_desc'])

            # Sort the facilities
            facilities = sorted(facilities, key=sort_function, reverse=reverse_sort)

            logger.debug(f"Sorted facilities by {sort_by}")

            # Return results template
            return render_template('results_stars_only.html', facilities=facilities, db_status=db_status, search_params={
                'specialty': specialty,
                'region': region,
                'min_quality': min_quality,
                'query_text': query_text,
                'original_query': original_query if detected_location else query_text,
                'detected_location': detected_location,
                'mapped_specialties': mapped_specialties,
                'sort_by': sort_by,
                'is_address_search': False
            })

    @app.route('/data-manager')
    def data_manager():
        """Data loading management dashboard - PROTECTED ADMIN AREA"""
        # Check for admin access: use a secret query parameter as a simple protection method
        # In production, this would be replaced with proper authentication
        admin_key = request.args.get('admin_key')
        if admin_key != os.environ.get('ADMIN_KEY', 'Cq9K7pLmN3rT5vX8zBdAeYgF'):
            # If no valid admin key, redirect to home page
            flash("Accesso non autorizzato all'area di amministrazione.", "danger")
            return redirect(url_for('index'))
        
        # Get statistics about current data
        regions = get_regions()
        specialties = get_specialties()

        # Get database status
        db_status = get_database_status()

        # Count facilities
        try:
            total_facilities = db.session.query(MedicalFacility).count()
            geocoded_facilities = db.session.query(MedicalFacility).filter(MedicalFacility.geocoded == True).count()
            facilities_with_coords = db.session.query(MedicalFacility)\
                .filter(MedicalFacility.latitude != None)\
                .filter(MedicalFacility.longitude != None).count()
            
            geocoded_percentage = round((geocoded_facilities / total_facilities) * 100) if total_facilities > 0 else 0
        except:
            total_facilities = 0
            geocoded_facilities = 0
            facilities_with_coords = 0
            geocoded_percentage = 0

        # Calculate progress percentage (assume 20 regions is 100%)
        total_regions_count = len(regions)
        progress_percentage = min(int((total_regions_count / 20) * 100), 100)

        # Check which batches have been loaded
        # This is a simple check based on expected regions in each batch
        batch_regions = {
            0: ["Puglia", "Trentino", "Toscana", "Lazio", "Lombardia"],
            1: ["Sicilia", "Piemonte", "Campania", "Veneto", "Liguria"],
            2: ["Emilia Romagna", "Sardegna", "Marche", "Abruzzo", "Calabria"],
            3: ["Friuli-Venezia Giulia", "Umbria", "Basilicata", "Molise", "Valle d Aosta"]
        }

        batch_status = {}
        region_names = [r.name for r in regions]

        for batch_num, expected_regions in batch_regions.items():
            # Check if at least 3 regions from this batch exist in the database
            # (allowing for some flexibility if certain regions fail to load)
            found_regions = [r for r in expected_regions if any(er in r for er in region_names)]
            batch_status[batch_num] = len(found_regions) >= 3

        all_batches_loaded = all(batch_status.values())

        # Add commands for database operations
        full_db_init_command = "python initialize_database.py"
        geocode_command = "python geocode_facilities.py"

        return render_template(
            'data_manager.html',
            total_regions=total_regions_count,
            total_facilities=total_facilities,
            total_specialties=len(specialties),
            progress_percentage=progress_percentage,
            batch_status=batch_status,
            all_batches_loaded=all_batches_loaded,
            db_status=db_status,
            full_db_init_command=full_db_init_command,
            geocode_command=geocode_command,
            geocoded_facilities=geocoded_facilities,
            facilities_with_coords=facilities_with_coords,
            geocoded_percentage=geocoded_percentage
        )

    @app.route('/load-data')
    @app.route('/load-data/<int:batch>')
    def load_data_route(batch=0):
        # PROTECTED ADMIN ROUTE
        admin_key = request.args.get('admin_key')
        if admin_key != os.environ.get('ADMIN_KEY', 'Cq9K7pLmN3rT5vX8zBdAeYgF'):
            flash("Accesso non autorizzato all'area di amministrazione.", "danger")
            return redirect(url_for('index'))
            
        try:
            # We now support batch loading to avoid timeouts
            logger.info(f"Loading data batch {batch}")

            # Import the data_loader module
            import data_loader

            # Only clear database on the first batch
            if batch == 0:
                logger.info("This is the first batch, clearing database...")
            else:
                logger.info(f"Loading batch {batch}, continuing from previous batches")

            # Load the data with batch parameter for limiting regions
            stats = load_data(batch=batch)

            # Provide info about continuing the loading process
            total_batches = 4  # We'll split the 20 regions into 4 batches of 5 each

            if batch < total_batches - 1:
                next_batch = batch + 1
                flash(f"Batch {batch} loaded successfully! Added {stats['total']} facilities from {stats['regions']} regions. " +
                      f"Continue loading with batch {next_batch}.", "success")
                # Add a link to the next batch that includes the admin key
                next_batch_url = f"/load-data/{next_batch}?admin_key={admin_key}"
                flash(f"<a href='{next_batch_url}' class='btn btn-primary'>Load Next Batch</a>", "info")
            else:
                flash(f"Final batch loaded successfully! Added {stats['total']} facilities from {stats['regions']} regions. " +
                      f"All regions are now loaded.", "success")
        except Exception as e:
            logger.error(f"Error loading data batch {batch}: {str(e)}")
            logger.exception(e)  # Log the full exception for debugging
            db.session.rollback()
            flash(f"Error loading data batch {batch}: {str(e)}", "danger")

        # Redirect to the data manager page with admin key
        return redirect(f'/data-manager?admin_key={admin_key}')

    @app.context_processor
    def utility_processor():
        """Add utility functions to template context"""
        def format_quality(quality):
            if quality is None:
                return "N/A"
            return f"{quality:.1f}/5.0"

        return dict(format_quality=format_quality)
    
    @app.route('/methodology')
    def methodology():
        """Show detailed information about our rating methodology"""
        # Get data for the form dropdowns (regions and specialties)
        from data_loader import get_regions, get_specialties
        regions = get_regions()
        specialties = get_specialties()
        
        return render_template('methodology.html', regions=regions, specialties=specialties)

    @app.route('/geocode-facilities')
    @app.route('/geocode-facilities/<int:batch_size>')
    def geocode_facilities_route(batch_size=10):
        """Geocode facilities and store their coordinates in the database"""
        # PROTECTED ADMIN ROUTE
        admin_key = request.args.get('admin_key')
        if admin_key != os.environ.get('ADMIN_KEY', 'Cq9K7pLmN3rT5vX8zBdAeYgF'):
            flash("Accesso non autorizzato all'area di amministrazione.", "danger")
            return redirect(url_for('index'))
            
        try:
            # For large batches, start a background process to avoid timeouts
            if batch_size > 10:
                import subprocess
                import sys
                
                # Launch the background geocoding script for larger batches
                cmd = [sys.executable, 'background_geocoding.py', '--batch-size', str(5)]
                subprocess.Popen(cmd)
                
                flash(f"Started background geocoding process for facilities. Check the logs for progress.", "success")
                flash(f"Small batches are processed immediately, larger ones in the background to avoid timeouts.", "info")
                
                logger.info(f"Started background geocoding process for {batch_size} facilities")
                
                # Get current stats for display
                from geocode_facilities import get_geocoding_statistics
                stats = get_geocoding_statistics()
                flash(f"Current stats: {stats['geocoded']}/{stats['total']} facilities have been geocoded.", "info")
                
                return redirect(request.referrer or '/')
            
            # Import the geocoding module
            from geocode_facilities import geocode_facilities
            
            # Get statistics before
            with app.app_context():
                facilities_before = db.session.query(MedicalFacility).filter(MedicalFacility.geocoded == True).count()
                coords_before = db.session.query(MedicalFacility).filter(
                    MedicalFacility.latitude != None, 
                    MedicalFacility.longitude != None
                ).count()
            
            # For smaller batches, process directly with a lower batch size
            # and shorter delay to avoid timeouts
            geocode_facilities(batch_size=5, max_facilities=batch_size)
            
            # Get statistics after
            with app.app_context():
                facilities_after = db.session.query(MedicalFacility).filter(MedicalFacility.geocoded == True).count()
                coords_after = db.session.query(MedicalFacility).filter(
                    MedicalFacility.latitude != None, 
                    MedicalFacility.longitude != None
                ).count()
            
            # Calculate how many were processed
            facilities_processed = facilities_after - facilities_before
            coords_added = coords_after - coords_before
            
            flash(f"Geocoded {facilities_processed} facilities, with {coords_added} successful coordinate lookups.", "success")
            
            # Remind about background processing option
            flash(f"For full background geocoding, use: python background_geocoding.py --continuous", "info")
            
            logger.info(f"Geocoded {facilities_processed} facilities (web batch), added {coords_added} coordinates")
            
        except Exception as e:
            logger.error(f"Error geocoding facilities: {str(e)}")
            logger.exception(e)
            flash(f"Error geocoding facilities: {str(e)}", "danger")
        
        # Redirect to the data manager page with admin key preserved
        return redirect(f'/data-manager?admin_key={admin_key}')
    
    @app.route('/download-db')
    def download_database():
        """Download the PostgreSQL database as CSV files in a ZIP archive"""
        # PROTECTED ADMIN ROUTE
        admin_key = request.args.get('admin_key')
        if admin_key != os.environ.get('ADMIN_KEY', 'Cq9K7pLmN3rT5vX8zBdAeYgF'):
            flash("Accesso non autorizzato all'area di amministrazione.", "danger")
            return redirect(url_for('index'))
            
        import os
        import tempfile
        from flask import send_file
        from export_database import export_database
        
        try:
            # Show a message in the logs
            logger.info("Starting database export to CSV/ZIP format...")
            
            # Export the database to CSV files and zip them
            export_file = export_database()
            
            if export_file and os.path.exists(export_file):
                logger.info(f"Database export successful: {export_file}")
                # Return the file for download
                return send_file(
                    export_file,
                    mimetype='application/zip',
                    as_attachment=True,
                    download_name=os.path.basename(export_file)
                )
            else:
                logger.error("Failed to export database: No file was created")
                return "Failed to export database. The export process did not create a file.", 500
        except Exception as e:
            logger.error(f"Error exporting database: {str(e)}")
            return f"Error exporting database: {str(e)}", 500
    
    # SEO friendly routes
    @app.route('/robots.txt')
    def robots():
        """Serve robots.txt file"""
        return send_file('static/robots.txt')
    
    @app.route('/sitemap.xml')
    def sitemap():
        """Serve sitemap.xml file"""
        return send_file('static/sitemap.xml')
        
    @app.route('/urls')
    def url_list():
        """Serve a list of all important URLs for crawler discovery"""
        return send_file('static/urls.txt')
    
    @app.route('/.well-known/security.txt')
    def security_txt():
        """Security.txt file with contact information for security researchers"""
        return """Contact: mailto:sicurezza@findmycure-italia.it
Expires: 2026-04-09T00:00:00.000Z
Preferred-Languages: it, en
"""
    
    @app.route('/<region>/<specialty>')
    def region_specialty_search(region, specialty):
        """SEO-friendly URL structure for region+specialty search"""
        # Redirect to the search page with the same parameters
        return redirect(url_for('search', region=region, specialty=specialty))
    
    # Add dynamic structured data (JSON-LD) to all pages
    @app.context_processor
    def inject_json_ld():
        """Inject JSON-LD structured data into all templates"""
        def get_structured_data():
            # Default data for website (homepage and general pages)
            current_path = request.path
            
            # Base data for all pages
            base_data = {
                "@context": "https://schema.org",
                "@type": "WebSite",
                "name": "FindMyCure Italia",
                "url": "https://findmycure.it/",
                "description": "Trova e confronta strutture sanitarie in Italia con valutazioni reali basate su dati ufficiali.",
                "potentialAction": {
                    "@type": "SearchAction",
                    "target": "https://findmycure.it/search?query_text={search_term}",
                    "query-input": "required name=search_term"
                }
            }
            
            # Enhanced structured data for search results pages
            if current_path == '/search':
                query = request.args.get('query_text', '')
                specialty = request.args.get('specialty', '')
                region = request.args.get('region', '')
                
                # Customize page title based on search parameters
                search_title = "Risultati della ricerca"
                if query:
                    search_title = f"Strutture per '{query}'"
                elif specialty:
                    search_title = f"Strutture di {specialty}"
                if region:
                    search_title += f" in {region}"
                    
                base_data = {
                    "@context": "https://schema.org",
                    "@type": "SearchResultsPage",
                    "name": f"FindMyCure Italia - {search_title}",
                    "description": f"Risultati della ricerca per strutture sanitarie in Italia: {search_title}"
                }
            
            # Enhanced structured data for methodology page
            elif current_path == '/methodology':
                base_data = {
                    "@context": "https://schema.org",
                    "@type": "Article",
                    "headline": "Metodologia di valutazione delle strutture sanitarie",
                    "description": "Scopri come valutiamo le strutture sanitarie italiane usando dati ufficiali del Programma Nazionale Esiti (PNE) e altre fonti verificate",
                    "datePublished": "2025-04-09",
                    "publisher": {
                        "@type": "Organization",
                        "name": "FindMyCure Italia",
                        "url": "https://findmycure.it/"
                    }
                }
            
            # Enhanced structured data for landing pages
            elif current_path in ['/cardiologia', '/oncologia', '/milano-strutture-sanitarie']:
                page_info = {
                    '/cardiologia': {
                        "name": "Strutture di Cardiologia in Italia",
                        "specialty": "Cardiologia"
                    },
                    '/oncologia': {
                        "name": "Strutture di Oncologia in Italia",
                        "specialty": "Oncologia"
                    },
                    '/milano-strutture-sanitarie': {
                        "name": "Strutture Sanitarie a Milano",
                        "city": "Milano"
                    }
                }
                
                info = page_info[current_path]
                
                base_data = {
                    "@context": "https://schema.org",
                    "@type": "MedicalWebPage",
                    "headline": info["name"],
                    "specialty": info.get("specialty", ""),
                    "about": {
                        "@type": "MedicalOrganization",
                        "name": "Strutture mediche italiane",
                        "address": {
                            "@type": "PostalAddress",
                            "addressCountry": "IT",
                            "addressRegion": info.get("city", "Italia")
                        }
                    }
                }
                
            return base_data
            
        return dict(get_structured_data=get_structured_data)
        
    # SEO landing pages for common specialties and cities
    @app.route('/cardiologia')
    def cardiology_landing():
        """Landing page for cardiology specialty searches"""
        return render_template('landing_page.html',
            title="Strutture di Cardiologia in Italia",
            headline="Trova le migliori strutture di cardiologia in Italia",
            description="Cerca e confronta ospedali e strutture specializzate in cardiologia in tutta Italia, con valutazioni basate su dati pubblici ufficiali.",
            search_url="/search?specialty=Cardiologia",
            content_title="Come trovare la migliore struttura di cardiologia",
            content_html="""
                <p>La cardiologia è una branca della medicina che si occupa della diagnosi e del trattamento delle malattie cardiovascolari, ovvero quelle che interessano il cuore e i vasi sanguigni.</p>
                <p>Quando si cerca una struttura specializzata in cardiologia, è importante valutare diversi fattori:</p>
                <ul>
                    <li>La presenza di cardiologi esperti e qualificati</li>
                    <li>La disponibilità di tecnologie diagnostiche avanzate</li>
                    <li>Il volume di interventi cardiovascolari eseguiti annualmente</li>
                    <li>I tassi di successo degli interventi</li>
                    <li>La vicinanza alla propria abitazione</li>
                </ul>
                <p>Tutte le strutture presenti nel nostro database sono valutate secondo criteri oggettivi basati sui dati del Programma Nazionale Esiti e altre fonti ufficiali.</p>
            """,
            keywords=["cardiologia milano", "cardiologo roma", "ospedali cardiochirurgia", "centri cardiologia", "malattie cardiovascolari", "ecocardiogramma"],
            facts=[
                "Le malattie cardiovascolari sono la prima causa di morte in Italia",
                "Un centro di eccellenza in cardiologia deve avere un volume adeguato di interventi",
                "La prossimità geografica è importante per le emergenze cardiache",
                "La mortalità a 30 giorni dopo intervento è un indicatore chiave di qualità",
                "I tempi di attesa per procedure cardiologiche variano tra le strutture"
            ],
            cta_text="cardiologiche"
        )
        
    @app.route('/oncologia')
    def oncology_landing():
        """Landing page for oncology specialty searches"""
        return render_template('landing_page.html',
            title="Strutture di Oncologia in Italia",
            headline="Cerca strutture specializzate in oncologia in Italia",
            description="Trova ospedali e centri oncologici in tutta Italia con valutazioni basate su dati pubblici ufficiali e indicatori di qualità verificati.",
            search_url="/search?specialty=Oncologia",
            content_title="Come scegliere un centro oncologico di qualità",
            content_html="""
                <p>L'oncologia è la specialità medica che si occupa dello studio e del trattamento dei tumori. Scegliere la struttura giusta per un trattamento oncologico è una decisione importante che può influenzare significativamente il percorso di cura.</p>
                <p>I fattori da considerare nella scelta di un centro oncologico includono:</p>
                <ul>
                    <li>La specializzazione nel tipo specifico di tumore da trattare</li>
                    <li>Il volume di casi trattati annualmente</li>
                    <li>La disponibilità di tecnologie diagnostiche e terapeutiche all'avanguardia</li>
                    <li>La presenza di un'équipe multidisciplinare</li>
                    <li>I tassi di sopravvivenza e la qualità della vita post-trattamento</li>
                </ul>
                <p>FindMyCure Italia ti aiuta a confrontare i centri oncologici su tutto il territorio nazionale, utilizzando dati verificati e indicatori di qualità oggettivi.</p>
            """,
            keywords=["oncologia roma", "centri oncologici milano", "terapie tumorali", "ospedali cancro", "oncologo napoli", "centro tumori"],
            facts=[
                "I centri ad alto volume hanno generalmente risultati migliori nel trattamento oncologico",
                "Un approccio multidisciplinare è fondamentale per il trattamento del cancro",
                "La specializzazione del centro nel tipo specifico di tumore è cruciale",
                "I tassi di sopravvivenza a 5 anni variano significativamente tra i centri",
                "L'accesso a trial clinici può offrire opzioni terapeutiche innovative"
            ],
            cta_text="oncologiche"
        )
    
    @app.route('/milano-strutture-sanitarie')
    def milan_landing():
        """Landing page for Milan healthcare facilities"""
        return render_template('landing_page.html',
            title="Strutture Sanitarie a Milano",
            headline="Trova le migliori strutture sanitarie a Milano",
            description="Cerca e confronta ospedali, cliniche e centri specializzati a Milano e provincia, con informazioni dettagliate e valutazioni di qualità.",
            search_url="/search?query_text=Milano",
            content_title="Servizi sanitari a Milano",
            content_html="""
                <p>Milano è una delle città italiane con la più alta concentrazione di strutture sanitarie di eccellenza, che offrono cure di alta qualità in numerose specialità mediche.</p>
                <p>Il sistema sanitario milanese include:</p>
                <ul>
                    <li>Ospedali pubblici (ASST) con reparti di eccellenza</li>
                    <li>Istituti di Ricovero e Cura a Carattere Scientifico (IRCCS)</li>
                    <li>Policlinici universitari</li>
                    <li>Strutture private convenzionate</li>
                    <li>Centri specialistici per specifiche patologie</li>
                </ul>
                <p>Utilizzando il nostro strumento di ricerca, puoi trovare la struttura più adatta alle tue esigenze nella zona di Milano, con valutazioni basate su dati ufficiali e indicatori di qualità verificati.</p>
            """,
            keywords=["ospedali milano", "cliniche private milano", "policlinico milano", "strutture sanitarie lombardia", "san raffaele milano", "niguarda"],
            facts=[
                "Milano ospita alcuni degli ospedali più rinomati d'Italia",
                "Molte strutture milanesi sono anche centri di ricerca e formazione",
                "Le strutture sono distribuite in diverse zone della città e provincia",
                "La Lombardia ha uno dei sistemi sanitari più efficienti d'Italia",
                "Molti ospedali milanesi sono specializzati in specifiche aree mediche"
            ],
            cta_text="a Milano"
        )