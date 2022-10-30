# Data Lake with Spark

Build an ELT pipeline for a Sparkify database hosted on Redshift. 
The python scripts load data from S3 JSON log files to Spark cluster and create the fact and dimension tables. 

## Usage

Run the scripts:

python etl.py

## File descriptions

* etl.py: Loads files into Spark and transforms them into fact and dimension tables.  
* dl.cfg: Spark cluster login info

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

## Database schema

![Schema](/Data_Warehouse_Redshift/schema.png?raw=true "Schema")