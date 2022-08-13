# Data Modeling with Postgres

Create a Postgres database for Sparkify with tables designed to optimize queries on song play analysis. 
The python scripts create a database schema and ETL pipeline for this analysis. 
Data is extracted from JSON log files, then processed and compiled into various database tables.

## Usage

Run the scripts in order:

python create_tables.py  
python etl.py

## File descriptions

* create_tables.py: Drops and creates tables. Run this file to reset tables before each time run ETL scripts.  
* test.ipynb: Displays the first few rows of each table to check database and running sanity checks.  
* etl.ipynb: Reads and processes a single file from song_data and log_data and loads the data into tables.  
* etl.py: Reads and processes files from song_data and log_data and loads them into tables.  
* sql_queries.py: Contains all sql queries, and is imported into the last three files above.
