# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id int PRIMARY KEY, start_time timestamp NOT NULL, 
                                                                user_id int NOT NULL, level varchar, song_id varchar, 
                                                                artist_id varchar, session_id int, location varchar, user_agent varchar);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, title varchar NOT NULL, 
                                                        artist_id varchar NOT NULL, year int, duration double precision NOT NULL);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, name varchar NOT NULL, location varchar, 
                                                            latitude double precision, longitude double precision);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, hour int NOT NULL, day int NOT NULL, 
                        week int NOT NULL, month int NOT NULL, year int NOT NULL, weekday int NOT NULL);
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")

# FIND SONGS

# The query with JOIN ... ON should work, but I keep getting syntax error. WHY?????
# ProgrammingError: syntax error at or near "ON"
# LINE 1: ... song_id, songs.artist_id FROM songs JOIN artists ON songs.a...

# song_select = ("""SELECT song_id, songs.artist_id FROM songs JOIN artists ON songs.artist_id=artists.artist_id
#                         WHERE songs.title=%s AND artists.name=%s AND songs.duration=%s;
# """)

# This query a temp fix, should do the same
song_select = ("""SELECT song_id, songs.artist_id 
                FROM artists, songs 
                WHERE songs.artist_id=artists.artist_id AND songs.title=%s AND artists.name=%s AND songs.duration=%s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]