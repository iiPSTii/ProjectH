#!/usr/bin/env python3
"""
Background Geocoding Script

This script is designed to be run as a background process to geocode facilities.
It periodically checks for facilities that need geocoding and processes them in
small batches to avoid hitting API rate limits.

Usage:
  python background_geocoding.py [--continuous] [--batch-size=N] [--delay=S]
  
  --continuous: Run continuously, checking for new facilities to geocode
  --batch-size=N: Process N facilities at a time (default: 5)
  --delay=S: Sleep S seconds between batches (default: 60)
"""

import time
import logging
import argparse
import signal
import sys
from app import app
from geocode_facilities import geocode_facilities, get_geocoding_statistics

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('geocoding.log')
    ]
)
logger = logging.getLogger(__name__)

# Default settings
DEFAULT_BATCH_SIZE = 5
DEFAULT_DELAY = 60  # seconds
running = True

def signal_handler(sig, frame):
    """Handle CTRL+C gracefully"""
    global running
    logger.info("Received termination signal. Finishing current batch then exiting...")
    running = False

def run_geocoder(batch_size=DEFAULT_BATCH_SIZE, delay=DEFAULT_DELAY, continuous=False):
    """
    Run the geocoder in a background process
    
    Args:
        batch_size: Number of facilities to process in each batch
        delay: Seconds to wait between batches
        continuous: Whether to run continuously or just once
    """
    logger.info(f"Starting background geocoder with batch_size={batch_size}, delay={delay}s, continuous={continuous}")
    
    try:
        # Get initial statistics
        stats = get_geocoding_statistics()
        logger.info(f"Initial stats: {stats['geocoded']}/{stats['total']} facilities geocoded")
        
        while running:
            # Process a batch of facilities
            logger.info(f"Processing batch of up to {batch_size} facilities...")
            result = geocode_facilities(batch_size=batch_size, max_facilities=batch_size)
            
            # Check if we have more facilities to process
            stats = get_geocoding_statistics()
            facilities_remaining = stats['total'] - stats['geocoded']
            
            if facilities_remaining <= 0:
                logger.info("All facilities have been geocoded!")
                if continuous:
                    logger.info(f"Sleeping for {delay} seconds before checking for new facilities...")
                    time.sleep(delay)
                else:
                    logger.info("One-time run complete. Exiting.")
                    break
            else:
                logger.info(f"{facilities_remaining} facilities still need geocoding")
                logger.info(f"Sleeping for {delay} seconds before next batch...")
                time.sleep(delay)
                
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Exiting...")
    except Exception as e:
        logger.exception(f"Error in background geocoder: {str(e)}")
    
    logger.info("Background geocoder stopped")

if __name__ == "__main__":
    # Handle signals
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Background geocoding process")
    parser.add_argument("--continuous", action="store_true", help="Run continuously, checking for new facilities")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help=f"Facilities per batch (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--delay", type=int, default=DEFAULT_DELAY, help=f"Seconds between batches (default: {DEFAULT_DELAY})")
    
    args = parser.parse_args()
    
    run_geocoder(batch_size=args.batch_size, delay=args.delay, continuous=args.continuous)