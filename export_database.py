"""
Database Export Script for FindMyCure Italia

This script exports the database content to a SQL file that can be easily downloaded.
It uses the pg_dump utility to create a SQL backup of your PostgreSQL database.

Usage:
  python export_database.py
"""

import os
import subprocess
import datetime

def export_database():
    """Export the database to a SQL file"""
    print("Exporting database to SQL file...")
    
    # Get database connection details from environment variables
    db_url = os.environ.get("DATABASE_URL")
    pg_host = os.environ.get("PGHOST")
    pg_port = os.environ.get("PGPORT")
    pg_user = os.environ.get("PGUSER")
    pg_password = os.environ.get("PGPASSWORD")
    pg_database = os.environ.get("PGDATABASE")
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"medical_facilities_backup_{timestamp}.sql"
    
    try:
        # Set PGPASSWORD environment variable for pg_dump
        env = os.environ.copy()
        # Make sure pg_password is not None before setting it
        pg_password_value = pg_password if pg_password is not None else ""
        env["PGPASSWORD"] = pg_password_value
        
        # Run pg_dump command
        cmd = [
            "pg_dump",
            f"--host={pg_host or 'localhost'}",
            f"--port={pg_port or '5432'}",
            f"--username={pg_user or 'postgres'}",
            "--format=p",  # plain text format
            "--file=" + output_file,
            pg_database or ''
        ]
        
        subprocess.run(cmd, env=env, check=True)
        print(f"Database successfully exported to {output_file}")
        print(f"File size: {os.path.getsize(output_file) / (1024*1024):.2f} MB")
        print(f"\nYou can download this file from the Replit Files panel.")
        
        return output_file
    except subprocess.CalledProcessError as e:
        print(f"Error exporting database: {e}")
        return None

if __name__ == "__main__":
    export_database()