import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop  = "DROP table IF EXISTS staging_songs"
songplay_table_drop       = "DROP table IF EXISTS songplays CASCADE"
user_table_drop           = "DROP table IF EXISTS users"
song_table_drop           = "DROP table IF EXISTS songs"
artist_table_drop         = "DROP table IF EXISTS artists"
time_table_drop           = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    artist          varchar,  
    auth            varchar,
    first_name      varchar,
    gender          char(1),
    item_in_session int,
    last_name       varchar,
    length          varchar,
    level           varchar,
    location        varchar,
    method          varchar,
    page            varchar,
    registration    bigint,
    session_id      int,
    title           varchar,
    status          varchar,
    ts              timestamp,
    user_agent      varchar,
    user_id         int);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int,
    artist_id varchar,
    latitude  float,
    longitude float,
    location  varchar,
    artist    varchar,
    song_id   varchar,
    title     varchar,
    duration  numeric,
    year      int);
""")

# Use BYTEDICT compression encoding for the following:
# level, gender, year and weekday: contains limited number of unique values

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int       IDENTITY(0,1) PRIMARY KEY, 
    start_time  timestamp NOT NULL DISTKEY SORTKEY REFERENCES time(start_time), 
    user_id     int       NOT NULL                 REFERENCES users(user_id), 
    level       varchar   ENCODE BYTEDICT, 
    song_id     varchar                            REFERENCES songs(song_id), 
    artist_id   varchar                            REFERENCES artists(artist_id),
    session_id  int, 
    location    varchar, 
    user_agent  varchar)
    DISTSTYLE KEY;
""") 

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id    int     PRIMARY KEY SORTKEY, 
    first_name varchar, 
    last_name  varchar, 
    gender     char(1) ENCODE BYTEDICT, 
    level      varchar ENCODE BYTEDICT);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id   varchar PRIMARY KEY SORTKEY, 
    title     varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year      int     ENCODE BYTEDICT, 
    duration  numeric NOT NULL);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY SORTKEY, 
    name varchar, 
    location varchar, 
    latitude float, 
    longitude float);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY DISTKEY SORTKEY, 
    hour       int, 
    day        int, 
    week       int, 
    month      int, 
    year       int ENCODE BYTEDICT, 
    weekday    int ENCODE BYTEDICT)
    DISTSTYLE KEY;
""")

# STAGING TABLES

# Use the following Data conversion parameters for the staging_events COPY command

# FORMAT AS JSON JSONPath: uses the named JSONPath file to map the data elements in the JSON source data to the columns in the target table.

# TIMEFORMAT AS 'epochmillisecs': specifies the time format that represented as epoch time, that is the number of milliseconds since January 1, 1970, 00:00:00 UTC

# TRUNCATECOLUMNS: truncates data in columns to the appropriate number of characters so that it fits the column specification

# BLANKSASNULL: loads blank fields, which consist of only white space characters, as NULL

# EMPTYASNULL: indicates that Amazon Redshift should load empty CHAR and VARCHAR fields as NULL

staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
FORMAT AS JSON {}
TIMEFORMAT AS 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])


# Use the following Data conversion parameters for the staging_songs COPY command

# FORMAT AS JSON 'auto': attempts to match all columns in the target table to JSON field name keys.

staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE {}
FORMAT AS JSON 'auto'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
REGION 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
       start_time, 
       user_id, 
       level, 
       song_id, 
       artist_id, 
       session_id, 
       location, 
       user_agent)
SELECT DISTINCT e.ts AS start_time,
       e.user_id,
       e.level,
       s.song_id,
       s.artist_id,
       e.session_id,
       e.location,
       e.user_agent
FROM staging_events e
JOIN staging_songs s
ON (s.title = e.title AND s.artist = e.artist)
AND e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (
       user_id,
       first_name,
       last_name, 
       gender, 
       level)
SELECT DISTINCT user_id, 
       first_name, 
       last_name, 
       gender, 
       level
FROM staging_events
WHERE user_id IS NOT NULL
AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (
       song_id, 
       title, 
       artist_id, 
       year, 
       duration)
SELECT DISTINCT song_id, 
       title, 
       artist_id, 
       year, 
       duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (
       artist_id, 
       name, 
       location, 
       latitude, 
       longitude)
SELECT DISTINCT artist_id, 
       artist, 
       location, 
       latitude, 
       longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (
       start_time, 
       hour, 
       day, 
       week, 
       month, 
       year, 
       weekday)
SELECT DISTINCT ts                    AS start_time,
       EXTRACT(HOUR FROM start_time)  AS hour,
       EXTRACT(DAY FROM start_time)   AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time)  AS year,
       EXTRACT(DOW FROM start_time)   As weekday       
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
