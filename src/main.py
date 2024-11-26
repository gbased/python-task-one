import argparse
from loader import DataLoader
from queries import DatabaseQueries
from exporter import DataExporter
from logger import setup_logging
import logging

def main():
    setup_logging() 
    logging.info('Starting the application')
    
    parser = argparse.ArgumentParser(description='Process dormitory data')
    parser.add_argument('--students', required=True, help='Students file path (JSON format)')
    parser.add_argument('--rooms', required=True, help='Rooms file path (JSON format)')
    parser.add_argument('--format', choices=['json', 'xml'], required=True, help='Output format (JSON or XML)')
    
    arg = parser.parse_args()
    
    loader = DataLoader()
    loader.load_students(arg.students)
    loader.load_rooms(arg.rooms)
    
    queries = DatabaseQueries()
    results = queries.execute_all_queries()
    
    exporter = DataExporter(arg.output)
    exporter.export(results)
    
    logging.info('Application completed')
    if 'queries' in locals():
            queries.close_connection()
    
if __name__ == '__main__':
    main()
