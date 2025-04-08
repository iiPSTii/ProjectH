"""
Database Export Script for FindMyCure Italia

This script exports the database content to a CSV file format that can be easily downloaded.
It uses SQLAlchemy to directly query the database and export data to CSV files.

Usage:
  python export_database.py
"""

import os
import datetime
import csv
import zipfile
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

def export_database():
    """Export the database tables to CSV files and compress them into a zip file"""
    print("Exporting database to CSV files...")
    
    # Get database connection details from environment variables
    db_url = os.environ.get("DATABASE_URL")
    
    if not db_url:
        print("Database URL not found in environment variables")
        return None
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"medical_facilities_export_{timestamp}"
    output_zip = f"{output_dir}.zip"
    
    # Create directory for export files
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    try:
        # Connect to the database
        engine = create_engine(db_url)
        metadata = MetaData()
        metadata.reflect(bind=engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Export each table to a CSV file
        for table_name, table in metadata.tables.items():
            export_table_to_csv(session, table, os.path.join(output_dir, f"{table_name}.csv"))
        
        # Create a zip file containing all CSV files
        with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(output_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    zipf.write(file_path, os.path.relpath(file_path, output_dir))
        
        print(f"Database successfully exported to {output_zip}")
        print(f"File size: {os.path.getsize(output_zip) / (1024*1024):.2f} MB")
        
        return output_zip
    except Exception as e:
        print(f"Error exporting database: {e}")
        return None

def export_table_to_csv(session, table, output_file):
    """Export a table to a CSV file"""
    print(f"Exporting table {table.name} to {output_file}...")
    
    # Get column names
    column_names = [column.name for column in table.columns]
    
    # Query all data from the table
    query = session.query(table).all()
    
    # Write data to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(column_names)  # Write header
        
        for row in query:
            # Convert row to a dictionary
            row_dict = {col: getattr(row, col) for col in column_names}
            # Write row data
            writer.writerow([row_dict[col] for col in column_names])
    
    print(f"Exported {len(query)} rows from {table.name}")

if __name__ == "__main__":
    export_database()