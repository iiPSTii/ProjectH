import os
import logging
from flask import Flask, render_template, request, jsonify, flash, redirect
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase
from werkzeug.middleware.proxy_fix import ProxyFix
from medical_mapping import map_query_to_specialties

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
        
        logger.debug(f"Search params: specialty={specialty}, region={region}, min_quality={min_quality}, query_text={query_text}")
        
        # Get database status
        db_status = get_database_status()
        
        # Build the query
        query = db.session.query(MedicalFacility)
        
        # Apply specialty filter if provided by form
        if specialty:
            normalized_specialty = normalize_specialty(specialty)
            query = query.join(MedicalFacility.specialties).join(FacilitySpecialty.specialty).filter(
                Specialty.name.ilike(f'%{normalized_specialty}%')
            )
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
            # First, check if we can map this query to medical specialties
            mapped_specialties = map_query_to_specialties(query_text)
            logger.debug(f"Mapped query '{query_text}' to specialties: {mapped_specialties}")
            
            # If not already filtered by specialty form field and we have mapped specialties
            if not specialty_filter_applied and mapped_specialties:
                query = query.join(MedicalFacility.specialties).join(FacilitySpecialty.specialty).filter(
                    Specialty.name.in_(mapped_specialties)
                )
            # If no medical mapping found or specialty already filtered, do regular text search
            else:
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
        
        # Execute query
        facilities = query.all()
        logger.debug(f"Found {len(facilities)} facilities matching criteria")
        
        # Return results template
        return render_template('results.html', facilities=facilities, db_status=db_status, search_params={
            'specialty': specialty,
            'region': region,
            'min_quality': min_quality,
            'query_text': query_text,
            'mapped_specialties': mapped_specialties
        })

    @app.route('/data-manager')
    def data_manager():
        """Data loading management dashboard"""
        # Get statistics about current data
        regions = get_regions()
        specialties = get_specialties()
        
        # Get database status
        db_status = get_database_status()
        
        # Count facilities
        try:
            total_facilities = db.session.query(MedicalFacility).count()
        except:
            total_facilities = 0
        
        # Calculate progress percentage (assume 20 regions is 100%)
        total_regions_count = len(regions)
        progress_percentage = min(int((total_regions_count / 20) * 100), 100)
        
        # Check which batches have been loaded
        # This is a simple check based on expected regions in each batch
        batch_regions = {
            0: ["Puglia", "Trentino", "Toscana", "Lazio", "Lombardia"],
            1: ["Sicilia", "Piemonte", "Campania", "Veneto", "Liguria"],
            2: ["Emilia Romagna", "Sardegna", "Marche", "Abruzzo", "Calabria"],
            3: ["Friuli Venezia Giulia", "Umbria", "Basilicata", "Molise", "Valle d Aosta"]
        }
        
        batch_status = {}
        region_names = [r.name for r in regions]
        
        for batch_num, expected_regions in batch_regions.items():
            # Check if at least 3 regions from this batch exist in the database
            # (allowing for some flexibility if certain regions fail to load)
            found_regions = [r for r in expected_regions if any(er in r for er in region_names)]
            batch_status[batch_num] = len(found_regions) >= 3
        
        all_batches_loaded = all(batch_status.values())
        
        # Add command to run full database initialization
        full_db_init_command = "python initialize_database.py"
        
        return render_template(
            'data_manager.html',
            total_regions=total_regions_count,
            total_facilities=total_facilities,
            total_specialties=len(specialties),
            progress_percentage=progress_percentage,
            batch_status=batch_status,
            all_batches_loaded=all_batches_loaded,
            db_status=db_status,
            full_db_init_command=full_db_init_command
        )

    @app.route('/load-data')
    @app.route('/load-data/<int:batch>')
    def load_data_route(batch=0):
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
                # Add a link to the next batch
                next_batch_url = f"/load-data/{next_batch}"
                flash(f"<a href='{next_batch_url}' class='btn btn-primary'>Load Next Batch</a>", "info")
            else:
                flash(f"Final batch loaded successfully! Added {stats['total']} facilities from {stats['regions']} regions. " +
                      f"All regions are now loaded.", "success")
        except Exception as e:
            logger.error(f"Error loading data batch {batch}: {str(e)}")
            logger.exception(e)  # Log the full exception for debugging
            db.session.rollback()
            flash(f"Error loading data batch {batch}: {str(e)}", "danger")
        
        # Redirect to the data manager page instead of index
        return redirect('/data-manager')

    @app.context_processor
    def utility_processor():
        """Add utility functions to template context"""
        def format_cost(cost):
            if cost is None:
                return "N/A"
            return f"€{cost:.2f}"
        
        def format_quality(quality):
            if quality is None:
                return "N/A"
            return f"{quality:.1f}/5.0"
        
        return dict(format_cost=format_cost, format_quality=format_quality)
