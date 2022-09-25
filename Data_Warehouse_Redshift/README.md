# Data Warehouse with Redshift

Build an ETL pipeline for a Sparkify database hosted on Redshift. 
The python scripts load data from S3 JSON log files to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables. 

## Usage

Run the scripts in order:

python create_tables.py  
python etl.py

## File descriptions

* create_tables.py: Drops and creates tables. Run this file to reset tables before each time run ETL scripts.  
* etl.py: Loads files into staging tables, and transforms them into fact and dimension tables.  
* sql_queries.py: Contains all SQL statements, and is imported into the two files above.
* dwh.cfg: Redshift database and IAM role info

## Datasets

All data files are in JSON format.

### Song Dataset

Contains metadata and artist of the song.

#### File location

```
s3://udacity-dend/song_data
```

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

#### File location

```
s3://udacity-dend/log_data
```

#### File path

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

#### File content

![log-data](/Data_Warehouse_Redshift/log-data.png?raw=true "Log Data")

#### Log data json path

```
s3://udacity-dend/log_json_path.json
```

## Database schema

![Schema](/Data_Warehouse_Redshift/schema.png?raw=true "Schema")
