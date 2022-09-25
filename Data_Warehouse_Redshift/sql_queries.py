import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (artist VARCHAR DISTKEY, auth VARCHAR, firstName VARCHAR, gender VARCHAR, 
                                                                            itemInSession INT, lastName VARCHAR, length FLOAT8, level VARCHAR, 
                                                                            location VARCHAR, method VARCHAR, page VARCHAR, registration FLOAT8, 
                                                                            sessionId INT, song VARCHAR, status INT, ts BIGINT, 
                                                                            userAgent VARCHAR, userId INT);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (num_songs INT, artist_id VARCHAR, artist_latitude FLOAT8, 
                                                                            artist_longitude FLOAT8, artist_location VARCHAR, artist_name VARCHAR DISTKEY, 
                                                                            song_id VARCHAR, title VARCHAR, duration FLOAT8, year INT);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id INT IDENTITY(0, 1) PRIMARY KEY, 
                                                                    start_time TIMESTAMP DISTKEY NOT NULL REFERENCES time(start_time), 
                                                                    user_id INT NOT NULL REFERENCES users(user_id), 
                                                                    level VARCHAR, song_id VARCHAR REFERENCES songs(song_id), 
                                                                    artist_id VARCHAR REFERENCES artists(artist_id), session_id INT, 
                                                                    location VARCHAR, user_agent VARCHAR);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id INT PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, level VARCHAR) diststyle all;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR PRIMARY KEY, title VARCHAR NOT NULL, 
                                                            artist_id VARCHAR NOT NULL, year INT, duration FLOAT8 NOT NULL) diststyle all;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, location VARCHAR, 
                                                                latitude FLOAT8, longitude FLOAT8) diststyle all;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP DISTKEY PRIMARY KEY, hour INT NOT NULL, day INT NOT NULL, 
                                                            week INT NOT NULL, month INT NOT NULL, year INT NOT NULL, weekday INT NOT NULL);
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {} 
iam_role {}
region 'us-west-2'
format as json {};
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs from {} 
iam_role {}
region 'us-west-2'
format as json 'auto ignorecase';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

# songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
#                             SELECT timestamp 'epoch' + ts * interval '0.001 second' AS ts2, userId, level, song_id, artist_id, sessionId, location, userAgent 
#                             FROM staging_events LEFT OUTER JOIN staging_songs 
#                             ON (staging_events.song = staging_songs.title 
#                             AND staging_events.artist = staging_songs.artist_name
#                             AND staging_events.length = staging_songs.duration)
#                             WHERE userId IS NOT NULL
# """)

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                            SELECT timestamp 'epoch' + ts * interval '0.001 second' AS ts2, userId, level, song_id, artist_id, sessionId, location, userAgent 
                            FROM staging_events LEFT OUTER JOIN staging_songs 
                            ON (staging_events.song = staging_songs.title 
                            AND staging_events.artist = staging_songs.artist_name
                            AND staging_events.length = staging_songs.duration)
                            WHERE page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
                        SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events
                        WHERE userId IS NOT NULL
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
                        SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs 
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs 
""")

# time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
#                         SELECT DISTINCT timestamp 'epoch' + ts * interval '0.001 second' AS ts2, 
#                         date_part(hour, ts2), date_part(day, ts2), date_part(week, ts2), 
#                         date_part(month, ts2), date_part(year, ts2), date_part(dayofweek, ts2) 
#                         FROM staging_events
#                         WHERE userId IS NOT NULL
# """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                        SELECT DISTINCT start_time, date_part(hour, start_time), date_part(day, start_time), date_part(week, start_time), 
                        date_part(month, start_time), date_part(year, start_time), date_part(dayofweek, start_time) 
                        FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
insert_table_queries = [songplay_table_insert, time_table_insert]
