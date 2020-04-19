import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS events_staging (
                                 artist VARCHAR,
                                 auth VARCHAR,
                                 firstName VARCHAR,
                                 gender VARCHAR,
                                 itemInSession VARCHAR,
                                 lastName VARCHAR,
                                 length NUMERIC,
                                 level VARCHAR,
                                 location VARCHAR,
                                 method VARCHAR,
                                 page VARCHAR,
                                 registration NUMERIC,
                                 sessionId INT,
                                 song VARCHAR,
                                 status INT,
                                 ts BIGINT,
                                 userAgent VARCHAR,
                                 userid INT
                                 )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS songs_staging (
                                 num_songs INT,
                                 artist_id VARCHAR,
                                 artist_latitude NUMERIC,
                                 artist_longitude NUMERIC,
                                 artist_location VARCHAR,
                                 artist_name VARCHAR,
                                 song_id VARCHAR,
                                 title VARCHAR,
                                 duration FLOAT,
                                 year INT
                                 )
""")
#Fact Table
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                            songplay_id INT IDENTITY(0,1) PRIMARY KEY,
                            start_time TIMESTAMP,
                            user_id INT,
                            level VARCHAR,
                            song_id VARCHAR,
                            artist_id VARCHAR,
                            session_id INT,
                            location VARCHAR,
                            user_agent VARCHAR
                            )
""")
#Dimenson Tables
user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                        user_id VARCHAR PRIMARY KEY,
                        first_name VARCHAR,
                        last_name VARCHAR,
                        gender VARCHAR,
                        level VARCHAR
                        )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                        song_id VARCHAR PRIMARY KEY,
                        title VARCHAR,
                        artist_id VARCHAR,
                        year INT,
                        duration FLOAT
                        )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                          artist_id VARCHAR PRIMARY KEY,
                          name VARCHAR,
                          location VARCHAR,
                          latitude FLOAT,
                          longitude FLOAT
                          )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                        start_time TIMESTAMP PRIMARY KEY,
                        hour INT,
                        day INT,
                        week INT,
                        month INT,
                        year INT,
                        weekday INT
                        )
""")

# STAGING TABLES

staging_events_copy = (""" COPY events_staging
                           FROM {}
                           iam_role {}
                           FORMAT AS json {};
                           """).format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
                        COPY songs_staging
                        FROM {}
                        iam_role {}
                        FORMAT AS json 'auto';
                        """).format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (
                            start_time, user_id, level, song_id, artist_id, 
                            session_id, location, user_agent)
                    SELECT  DISTINCT(to_timestamp(to_char(es.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS')) as start_time,
                            es.userid,
                            es.level,
                            ss.song_id,
                            ss.artist_id,
                            es.sessionId,
                            es.location,
                            es.userAgent
                    FROM events_staging as es
                    JOIN songs_staging as ss
                    ON (ss.title = es.song AND ss.artist_name = es.artist)
                    AND (es.page = 'NextSong')

""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                SELECT  userid as user_id,
                        firstName as first_name,
                        lastName as last_name,
                        gender,
                        level
                FROM events_staging
                WHERE page = 'NextSong';
                        
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                SELECT song_id,
                       title,
                       artist_id,
                       year,
                       duration
                FROM songs_staging
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                SELECT artist_id,
                       artist_name,
                       artist_location,
                       artist_latitude,
                       artist_longitude
                FROM songs_staging
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day , week, month, year, weekday)
                SELECT ts AS start_time, 
                       EXTRACT(HOUR FROM ts) AS hour, 
                       EXTRACT(DAY FROM ts) AS day, 
                       EXTRACT(WEEK FROM ts) AS week, 
                       EXTRACT(MONTH FROM ts) AS month, 
                       EXTRACT(YEAR FROM ts) AS year, 
                       EXTRACT(WEEKDAY FROM ts) AS weekday
                       FROM
                       (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS ts 
                       FROM events_staging
                       )
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
