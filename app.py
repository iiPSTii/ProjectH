import os
import logging
from flask import Flask, render_template, request, jsonify, flash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase
from werkzeug.middleware.proxy_fix import ProxyFix

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
    from models import MedicalFacility, Specialty, FacilitySpecialty, Region
    db.create_all()
    
    # Import and register route functions
    from data_loader import load_data, get_regions, get_specialties, normalize_specialty

    @app.route('/')
    def index():
        regions = get_regions()
        specialties = get_specialties()
        return render_template('index.html', regions=regions, specialties=specialties)

    @app.route('/search')
    def search():
        # Get search parameters
        specialty = request.args.get('specialty', '')
        region = request.args.get('region', '')
        min_quality = request.args.get('min_quality', 0, type=float)
        max_cost = request.args.get('max_cost', 1000000, type=float)
        
        logger.debug(f"Search params: specialty={specialty}, region={region}, min_quality={min_quality}, max_cost={max_cost}")
        
        # Build the query
        query = db.session.query(MedicalFacility)
        
        # Apply filters
        if specialty:
            normalized_specialty = normalize_specialty(specialty)
            query = query.join(MedicalFacility.specialties).join(FacilitySpecialty.specialty).filter(
                Specialty.name.ilike(f'%{normalized_specialty}%')
            )
        
        if region:
            query = query.join(MedicalFacility.region).filter(Region.name.ilike(f'%{region}%'))
        
        if min_quality is not None and min_quality > 0:
            query = query.filter(MedicalFacility.quality_score >= min_quality)
            
        if max_cost is not None and max_cost < 1000000:
            query = query.filter(MedicalFacility.cost_estimate <= max_cost)
        
        # Execute query
        facilities = query.all()
        logger.debug(f"Found {len(facilities)} facilities matching criteria")
        
        # Return results template
        return render_template('results.html', facilities=facilities, search_params={
            'specialty': specialty,
            'region': region,
            'min_quality': min_quality,
            'max_cost': max_cost
        })

    @app.route('/load-data')
    def load_data_route():
        try:
            # We've simplified the data loading to always use sample data
            logger.info(f"Loading data with sample data")
            
            # Import the data_loader module
            import data_loader
            
            # Database clearing is now handled in the load_data function
            logger.info("Preparing to load data...")
            
            # Load the data with appropriate error handling
            stats = load_data()
            flash(f"Data loaded successfully using sample data! Added {stats['total']} facilities from {stats['regions']} regions.", "success")
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            logger.exception(e)  # Log the full exception for debugging
            db.session.rollback()
            flash(f"Error loading data: {str(e)}", "danger")
        
        return render_template('index.html', regions=get_regions(), specialties=get_specialties())

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
