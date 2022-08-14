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

## Datasets

All data files are in JSON format.

### Song Dataset

Contains metadata and artist of the song.

#### File path

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
#### File content

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### Log Dataset

Contains activity logs from a music streaming app.

#### File path

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
#### File content

![log-data](/log-data.png?raw=true "Log Data")