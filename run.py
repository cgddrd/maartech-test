# MAARTech technical test submission.
# Author: Connor Goddard
# First Published: 2021-05-20 

# Submission notes:
#   - For this task, I've made a two key assumptions: 1) we only need to support CSV file types; and 2) that it's a requirement to have the ORIGINAL/RAW data AS CONTAINED IN THE DATA FILES imported into the database tables.
#   
#   - I've made the decision NOT to transform the data and build new feature columns (e.g. combine the 'lat' and 'long' columns into a single GIS 'POINT' column) because in my experience, 
#   you would typically want to make sure the RAW data is imported 'as-is', and then apply such transformations across the 'raw' tables 
#   to curate new 'analytics' tables once the data is available in the database. This same reasoning led me to choose to NOT convert 
#   the hexadecimal representation of OSM tag values into plaintext. Again, this could be done as part of a downstream process, with the original data preserved.
#   
#  - I recognise that the data contained in the input files appears to be OpenStreetMap data, so it is possible that instead of connecting to and querying the database directly from Python, 
#   we could potentially make use of the official 'osm2pgsql' tool (https://osm2pgsql.org/) which could automate much of the table schema creation and unpacking. (This could even be called dynamically via a Python script.)
#   
#  - In terms of database credentials, in a production envrionment, we'd want to load the credentials in from a secure location at runtime (i.e. ideally from a secrets manager, 
#  but at the very least from a 'secure' configuration file - excluded from version control). 
# 
#  - I could have used SQLAlchemy to provide the connection to the database (SQLAlchemy is a popular and well-established library for working with RDBMS databases in Python), however,
#  because I wanted to take particular advantage of the 'COPY FROM' syntax supported by PostgresSQL, using SQL Alchemy would have been in some ways redundant, because I would have needed to 
#  access the underlying engine (psycopg2) in order to use the 'copy_expert()' function (i.e. it was more efficient just to import and use the psycopg2 library directly in this case).
# 
#  - I felt that building Python classes/objects in this situation was a little bit overkill, so kept everything contained inside a single script file with core functionality split out to dedicated functions. 
#  Obviously if the scope of the application was to grow (e.g. to parse and import different data file types), then abstracting certain logic (e.g. to load/parse these different file types) to dedicated 
#  class files would be a reasonable option.
# 
#  - In terms of evolving this application, I would like to add the ability to define the table schema directly from CSV header structure.


import click
from psycopg2 import connect, sql
from pathlib import Path
import yaml
import logging

# I'm a fan of using decorators for readabililty.
@click.command()
@click.option('--config', default='./config.yml', help='The path to the config file.')
def run(config):
    """Imports data from CSV files into a series of PostgresSQL tables (one table per file)."""

    logging.info('Application started.')

    db_conn = None

    # Personally, I prefer YAML format in defining configuration files vs. the standard 'INI' format provided by Python. I find it cleaner.
    config = read_yaml_config(config)

    files = list_files(config['target_path'])

    try:
        # Use the '**' syntax to flatten the dictionary into key-value pairs that can be passed into as parameters into psycopg2.connect().
        db_conn = connect(**config['db'])

        for file in files:
            import_file_to_database(file, db_conn)

        logging.info('Import complete.')

    except Exception:
        logging.error('An error occurred whilst importing data files into the database', exc_info=1)

    finally:
        if db_conn is not None:
            db_conn.close()

def read_yaml_config(config_path):

    try:
        with open(config_path) as file:
            #  We use safe_load() here to help prevent execution of any arbitary code embedded in the YAML file. 
            yaml_file = yaml.safe_load(file)
            return yaml_file
    except Exception:
        logging.error('Failed to load YAML config file.', exc_info=1)

def list_files(search_folder:str):

    pattern = "*.csv"

    directory = Path(search_folder)

    # Return a list of all files that match the pattern in the search folder.
    return [csvFile for csvFile in directory.glob(pattern)]

def import_file_to_database(file_path:str, conn):
    
    file_name = Path(file_path).stem

    try:

        logging.info('Importing file {} into database table {}.'.format(file_path, file_name))

        with conn.cursor() as cur:
            
            #  First, attempt to create the table if it doesn't already exist.
            query = sql.SQL("""
            
                CREATE TABLE IF NOT EXISTS {table_name} (
                    
                    osm_id INTEGER PRIMARY KEY,
                    area NUMERIC NOT NULL, 
                    lon NUMERIC NOT NULL, 
                    lat NUMERIC NOT NULL, 
                    tags JSONB, 
                    osm_type VARCHAR(25) NOT NULL, 
                    p_tag_value TEXT, 
                    city TEXT, 
                    postcode TEXT, 
                    address TEXT, 
                    street TEXT, 
                    has_way BOOLEAN NOT NULL,
                    shop_type TEXT,
                    derived_shared_area NUMERIC,
                    derived_way_area NUMERIC,
                    parent_way INTEGER, 
                    shared_divisor INTEGER, 
                    area_sq_foot NUMERIC NOT NULL
                )
            
            """).format(table_name = file_name)

            cur.execute(query)
            cur.commit()

            with open(file_path, 'r') as f: 

                # Second, use the PgSQL 'COPY' feature to efficiently copy the contects of the CSV file into the table. (This can scale to millions of rows.) - https://www.postgresql.org/docs/current/sql-copy.html
                query = sql.SQL("""
                            COPY {table_name} FROM stdin WITH CSV HEADER
                            DELIMITER as ','
                            """).format(table_name = file_name)

                cur.copy_expert(sql=query, file=f)

    except Exception: 
        logging.error('Failed to import file {} into database table {}'.format(file_path, file_name), exc_info=1)

    finally:
        if cur is not None:
            cur.close()

if __name__ == '__main__':
    run()